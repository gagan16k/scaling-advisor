// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package advisor

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	apicommontypes "github.com/gardener/scaling-advisor/api/common/types"
	configv1alpha1 "github.com/gardener/scaling-advisor/api/config/v1alpha1"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/api/minkapi"
	"github.com/gardener/scaling-advisor/api/minkapi/typeinfo"
	plannerapi "github.com/gardener/scaling-advisor/api/planner"
	pricingapi "github.com/gardener/scaling-advisor/api/pricing"
	"github.com/gardener/scaling-advisor/minkapi/view"
	"github.com/gardener/scaling-advisor/planner"
	"github.com/gardener/scaling-advisor/planner/scheduler"
	"github.com/gardener/scaling-advisor/pricing"

	"github.com/gardener/scaling-advisor/samples"
	"github.com/go-logr/logr"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Fallbacks used when OperatorConfig leaves the corresponding field unset, to
// keep configs like testdata/local-operator-config.yaml working unchanged.
// TODO: drop once a defaulter for ScalingAdviceGenerationConfig lands in
// api/config/v1alpha1/defaults.go and CloudProvider is required.
var (
	defaultSimulatorStrategy = apicommontypes.SimulatorStrategySingleNodeMultiSim
	defaultScoringStrategy   = apicommontypes.NodeScoringStrategyLeastCost
	defaultAdviceMode        = apicommontypes.ScalingAdviceGenerationModeAllAtOnce
	defaultCloudProvider     = apicommontypes.CloudProviderAWS
)

var runPlannerCounter atomic.Uint64

// plannerStack holds the planner's long-lived dependencies, built once at
// reconciler creation and reused across reconciles.
type plannerStack struct {
	pricingAccess     pricingapi.InstancePricingAccess
	schedulerLauncher plannerapi.SchedulerLauncher
	factories         plannerapi.Factories
	storageMeta       plannerapi.StorageMetaAccess
	simulatorConfig   plannerapi.SimulatorConfig
	simulatorStrategy apicommontypes.SimulatorStrategy
	scoringStrategy   apicommontypes.NodeScoringStrategy
	adviceMode        apicommontypes.ScalingAdviceGenerationMode
}

func newPlannerStack(opCfg *configv1alpha1.OperatorConfig) (*plannerStack, error) {
	pricingAccess, err := pricing.GetInstancePricingAccess(opCfg.InstancePricing.Provider, opCfg.InstancePricing.InstancePricingDataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load instance pricing: %w", err)
	}

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

	cloudProvider := opCfg.CloudProvider
	if cloudProvider == "" {
		cloudProvider = defaultCloudProvider
	}
	gen := opCfg.AdviceGeneration
	simStrategy := gen.SimulatorStrategy
	if simStrategy == "" {
		simStrategy = defaultSimulatorStrategy
	}
	scoringStrategy := gen.ScoringStrategy
	if scoringStrategy == "" {
		scoringStrategy = defaultScoringStrategy
	}
	adviceMode := gen.Mode
	if adviceMode == "" {
		adviceMode = defaultAdviceMode
	}

	return &plannerStack{
		pricingAccess:     pricingAccess,
		schedulerLauncher: schedulerLauncher,
		factories:         planner.NewFactories(),
		storageMeta:       &operatorStorageMetaAccess{provider: cloudProvider},
		simulatorConfig:   simulatorConfig,
		simulatorStrategy: simStrategy,
		scoringStrategy:   scoringStrategy,
		adviceMode:        adviceMode,
	}, nil
}

// runPlanner builds a per-reconcile ViewAccess (see plannerStack for why),
// runs one Plan call against it using the long-lived dependencies in
// r.planner, and returns the produced ScaleOutPlan.
func (r *Reconciler) runPlanner(
	ctx context.Context,
	log logr.Logger,
	snap plannerapi.ClusterSnapshot,
	constraint *corev1alpha1.ScalingConstraint,
) (*corev1alpha1.ScaleOutPlan, error) {

	// ViewAccess is intentionally NOT added to plannerStack: minkapi/view/access.go has no
	// RemoveSandboxView API, so a long-lived ViewAccess accumulates one
	// Request-<id> sandbox entry per reconcile.
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
	defer func() {
		if cerr := viewAccess.Close(); cerr != nil {
			log.Error(cerr, "failed to close minkapi ViewAccess")
		}
	}()

	scalingPlanner, err := r.planner.factories.Planner.NewPlanner(plannerapi.ScalingPlannerArgs{
		ViewAccess:        viewAccess,
		ResourceWeigher:   r.planner.factories.ResourceWeigher,
		PricingAccess:     r.planner.pricingAccess,
		SchedulerLauncher: r.planner.schedulerLauncher,
		StorageMetaAccess: r.planner.storageMeta,
		SimulatorConfig:   r.planner.simulatorConfig,
		SimulatorFactory:  r.planner.factories.Simulator,
		SimulationFactory: r.planner.factories.Simulation,
		TraceDir:          "",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create planner: %w", err)
	}

	requestID := fmt.Sprintf("%s-%d", constraint.Name, runPlannerCounter.Add(1))
	req := plannerapi.Request{
		RequestRef: plannerapi.RequestRef{
			ID:            requestID,
			CorrelationID: string(constraint.UID),
		},
		CreationTime:         time.Now(),
		Constraint:           constraint,
		Snapshot:             snap,
		SimulatorStrategy:    r.planner.simulatorStrategy,
		ScoringStrategy:      r.planner.scoringStrategy,
		AdviceGenerationMode: r.planner.adviceMode,
		DiagnosticVerbosity:  0,
	}

	// ScalingPlanner.Plan requires the caller to drain responseCh until close
	// to avoid leaking the producer goroutine; we keep the first plan and the
	// first error but continue draining.
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

// publishAdvice creates a ScalingAdvice resource carrying the produced ScaleOutPlan.
func (r *Reconciler) publishAdvice(
	ctx context.Context,
	log logr.Logger,
	constraint *corev1alpha1.ScalingConstraint,
	plan *corev1alpha1.ScaleOutPlan,
) error {
	advice := &corev1alpha1.ScalingAdvice{
		Spec: corev1alpha1.ScalingAdviceSpec{
			ScaleOutPlan: plan,
			ConstraintRef: apicommontypes.NamespacedName{
				Namespace: constraint.Namespace,
				Name:      constraint.Name,
			},
		},
	}
	advice.Name = fmt.Sprintf("scaling-advice-%s", uuid.NewUUID())
	advice.Namespace = constraint.Namespace

	if err := r.client.Create(ctx, advice); err != nil {
		return err
	}
	log.Info("published ScalingAdvice",
		"name", advice.Name,
		"namespace", advice.Namespace,
		"items", len(plan.Items),
		"unsatisfiedPods", len(plan.UnsatisfiedPodNames))
	return nil
}

// operatorStorageMetaAccess mirrors testStorageMetaAccess in planner/testutil:
// supplies a fallback CSINodeSpec via samples helpers for unseen instance types.
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

// Compile-time guard that ScalingAdvice is registered in the controller-runtime
// scheme (via api/core/v1alpha1.AddToScheme, invoked from manager.go).
var _ client.Object = (*corev1alpha1.ScalingAdvice)(nil)
