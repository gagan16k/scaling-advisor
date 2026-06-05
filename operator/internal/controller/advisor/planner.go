// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package advisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	apicommontypes "github.com/gardener/scaling-advisor/api/common/types"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/api/minkapi"
	"github.com/gardener/scaling-advisor/api/minkapi/typeinfo"
	plannerapi "github.com/gardener/scaling-advisor/api/planner"
	"github.com/gardener/scaling-advisor/minkapi/view"
	"github.com/gardener/scaling-advisor/planner"
	"github.com/gardener/scaling-advisor/planner/scheduler"
	pricingtestutil "github.com/gardener/scaling-advisor/pricing/testutil"
	"github.com/gardener/scaling-advisor/samples"
	"github.com/go-logr/logr"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Hardcoded planner inputs — these are stop-gap defaults. When OperatorConfig
// gains a planner section, replace these with values pulled from r.config.
//
// Sourced from planner/testutil/testutil.go, which is the canonical place that
// constructs a ScalingPlanner with the full set of dependencies.
var (
	plannerSimulatorStrategy = apicommontypes.SimulatorStrategySingleNodeMultiSim
	plannerScoringStrategy   = apicommontypes.NodeScoringStrategyLeastCost
	plannerAdviceMode        = apicommontypes.ScalingAdviceGenerationModeAllAtOnce
	plannerCloudProvider     = apicommontypes.CloudProviderAWS
)

// runPlannerCounter generates monotonically-increasing request IDs across
// reconcile invocations within a single operator process.
var runPlannerCounter atomic.Uint64

// runPlanner constructs a fresh planner stack for one reconcile cycle, runs it,
// and returns the produced ScaleOutPlan. Every dependency is constructed and
// torn down inside this call so a single reconcile leaves no goroutines or file
// descriptors behind.
//
// This deliberately mirrors the construction sequence in
// planner/testutil/testutil.go (rather than service/internal/core/service.go,
// which omits StorageMetaAccess and the simulator/simulation factories).
func (r *Reconciler) runPlanner(
	ctx context.Context,
	log logr.Logger,
	snap plannerapi.ClusterSnapshot,
	constraint *corev1alpha1.ScalingConstraint,
) (*corev1alpha1.ScaleOutPlan, error) {
	// 1. ViewAccess — pure in-process minkapi view repository. Not started as
	// an HTTP server; the planner only needs the local API.
	viewAccess, err := view.NewAccess(ctx, &minkapi.ViewArgs{
		Name:   minkapi.DefaultBasePrefix,
		Scheme: typeinfo.SupportedScheme,
		WatchConfig: minkapi.WatchConfig{
			QueueSize: minkapi.DefaultWatchQueueSize,
			Timeout:   minkapi.DefaultWatchTimeout,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minkapi ViewAccess: %w", err)
	}
	// ViewAccess holds the only goroutines/fds in the planner stack — closing
	// it tears down the watch fan-out and any per-view caches.
	defer func() {
		if cerr := viewAccess.Close(); cerr != nil {
			log.Error(cerr, "failed to close minkapi ViewAccess")
		}
	}()

	// 2. Pricing — the testutil loader reads embedded JSON for AWS top-20
	// instance types. Production deployments should replace this once
	// OperatorConfig grows a pricing-data path.
	pricingAccess, err := pricingtestutil.GetInstancePricingAccessForTop20AWSInstanceTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to load instance pricing: %w", err)
	}

	// 3. Scheduler launcher — parses an embedded bin-packing scheduler config.
	// No process is spawned at construction; embedded schedulers are launched
	// per-simulation and bounded by MaxParallelSimulations via a semaphore.
	schedulerConfigBytes, err := samples.LoadBinPackingSchedulerConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load bin-packing scheduler config: %w", err)
	}
	simulatorConfig := plannerapi.SimulatorConfig{
		MaxParallelSimulations:           plannerapi.DefaultMaxParallelSimulations,
		TrackPollInterval:                plannerapi.DefaultTrackPollInterval,
		MaxUnchangedTrackAttempts:        plannerapi.DefaultMaxUnchangedTrackAttempts,
		BindVolumeClaimsForImmediateMode: true,
	}
	schedulerLauncher, err := scheduler.NewLauncherFromConfig(
		schedulerConfigBytes,
		simulatorConfig.MaxParallelSimulations,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create scheduler launcher: %w", err)
	}

	// 4. Factories + planner.
	factories := planner.NewFactories()
	storageMeta := &operatorStorageMetaAccess{provider: plannerCloudProvider}
	scalingPlanner, err := factories.Planner.NewPlanner(plannerapi.ScalingPlannerArgs{
		ViewAccess:        viewAccess,
		ResourceWeigher:   factories.ResourceWeigher,
		PricingAccess:     pricingAccess,
		SchedulerLauncher: schedulerLauncher,
		StorageMetaAccess: storageMeta,
		SimulatorConfig:   simulatorConfig,
		SimulatorFactory:  factories.Simulator,
		SimulationFactory: factories.Simulation,
		TraceDir:          "",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create planner: %w", err)
	}

	// 5. Build the request and invoke. Every reconcile gets its own request ID
	// so log correlation works even on fast retries.
	requestID := fmt.Sprintf("%s-%d", constraint.Name, runPlannerCounter.Add(1))
	req := plannerapi.Request{
		RequestRef: plannerapi.RequestRef{
			ID:            requestID,
			CorrelationID: string(constraint.UID),
		},
		CreationTime:         time.Now(),
		Constraint:           constraint,
		Snapshot:             snap,
		SimulatorStrategy:    plannerSimulatorStrategy,
		ScoringStrategy:      plannerScoringStrategy,
		AdviceGenerationMode: plannerAdviceMode,
		DiagnosticVerbosity:  0,
	}

	// The planner contract (api/planner/types.go on ScalingPlanner.Plan) requires
	// the caller to drain responseCh until close to avoid leaking the producer
	// goroutine. We keep the first non-error plan but continue draining.
	responseCh := scalingPlanner.Plan(ctx, req)
	var plan *corev1alpha1.ScaleOutPlan
	var planErr error
	for resp := range responseCh {
		if resp.Error != nil {
			if planErr == nil {
				planErr = resp.Error
			}
			continue
		}
		if plan == nil && resp.ScaleOutPlan != nil {
			plan = resp.ScaleOutPlan
		}
	}
	if plan == nil && planErr != nil {
		return nil, fmt.Errorf("planner failed: %w", planErr)
	}
	return plan, nil
}

// publishAdvice creates a ScalingAdvice resource carrying the produced
// ScaleOutPlan. The name is derived from a hash of the input snapshot so
// repeated reconciles over identical state produce idempotent CR names —
// duplicate creates are tolerated as IsAlreadyExists.
func (r *Reconciler) publishAdvice(
	ctx context.Context,
	log logr.Logger,
	constraint *corev1alpha1.ScalingConstraint,
	plan *corev1alpha1.ScaleOutPlan,
	snap plannerapi.ClusterSnapshot,
) error {
	hash, err := snapshotHash(snap)
	if err != nil {
		return fmt.Errorf("failed to compute snapshot hash: %w", err)
	}
	advice := &corev1alpha1.ScalingAdvice{
		Spec: corev1alpha1.ScalingAdviceSpec{
			ScaleOutPlan: plan,
			ConstraintRef: apicommontypes.NamespacedName{
				Namespace: constraint.Namespace,
				Name:      constraint.Name,
			},
		},
	}
	advice.Name = fmt.Sprintf("scaling-advice-%s", hash)
	advice.Namespace = constraint.Namespace

	if err := r.client.Create(ctx, advice); err != nil {
		if apierrors.IsAlreadyExists(err) {
			log.V(1).Info("ScalingAdvice already exists for this snapshot; skipping",
				"name", advice.Name)
			return nil
		}
		return err
	}
	log.Info("published ScalingAdvice",
		"name", advice.Name,
		"namespace", advice.Namespace,
		"items", len(plan.Items),
		"unsatisfiedPods", len(plan.UnsatisfiedPodNames))
	return nil
}

// snapshotHash returns a short, stable hash of the cluster snapshot suitable
// for use in resource names. Uses JSON because all snapshot fields are JSON-
// tagged; the planner already round-trips the snapshot as JSON in tests.
func snapshotHash(snap plannerapi.ClusterSnapshot) (string, error) {
	data, err := json.Marshal(snap)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:6]), nil // 12 hex chars; collision-safe for our cardinality
}

// operatorStorageMetaAccess is a thin wrapper over samples helpers that
// supplies a fallback CSINodeSpec for instance types we have not yet seen
// CSI metadata for. Mirrors testStorageMetaAccess in planner/testutil.
type operatorStorageMetaAccess struct {
	provider apicommontypes.CloudProvider
}

var _ plannerapi.StorageMetaAccess = (*operatorStorageMetaAccess)(nil)

func (s *operatorStorageMetaAccess) GetFallbackCSINodeSpec(instanceType string) (storagev1.CSINodeSpec, error) {
	maxVolumes := samples.GetMaxAllocatableVolumes(s.provider, instanceType)
	drivers, err := samples.GetCSINodeDrivers(s.provider, maxVolumes)
	if err != nil {
		return storagev1.CSINodeSpec{}, err
	}
	return storagev1.CSINodeSpec{Drivers: drivers}, nil
}

// Compile-time check that the planner-facing controller-runtime client knows
// how to round-trip ScalingAdvice. The scheme is registered in
// api/core/v1alpha1/register.go via AddToScheme, which manager.go already
// invokes; this assignment exists purely to surface a build error if a future
// refactor drops the registration.
var _ client.Object = (*corev1alpha1.ScalingAdvice)(nil)
