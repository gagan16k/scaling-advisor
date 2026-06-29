// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package advisor

import (
	"context"
	"errors"
	"fmt"
	"time"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	configv1alpha1 "github.com/gardener/scaling-advisor/api/config/v1alpha1"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/operator/internal/controller/statetracker"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	componentName  = "operator-reconciler"
	controllerName = "scaling-advisor-controller"
)

const shortRetry = 5 * time.Second

// Reconciler reconciles ScalingConstraint against the latest in-memory ClusterSnapshot and
// (eventually) drives the embedded planner to produce ScalingAdvice. See docs/operator.md.
type Reconciler struct {
	client    client.Client
	log       logr.Logger
	config    configv1alpha1.ScalingAdviceControllerConfig
	csTracker *statetracker.ClusterStateTracker
	planner   *plannerStack
}

// NewReconciler eagerly builds the planner stack; failure here aborts manager startup.
func NewReconciler(mgr ctrl.Manager, opCfg *configv1alpha1.OperatorConfig, csBuilder *statetracker.ClusterStateTracker) (*Reconciler, error) {
	stack, err := newPlannerStack(opCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build planner stack: %w", err)
	}
	return &Reconciler{
		client:    mgr.GetClient(),
		log:       mgr.GetLogger().WithName(componentName),
		config:    opCfg.Controllers.ScalingAdvice,
		csTracker: csBuilder,
		planner:   stack,
	}, nil
}

// SetupWithManager wires the two reconcile triggers:
//  1. ScalingConstraint changes
//  2. Fallback heartbeat
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return builder.ControllerManagedBy(mgr).
		Named(controllerName).
		For(&corev1alpha1.ScalingConstraint{}).
		Complete(r)
}

// Reconcile implements. Its logic follows the steps:
//  1. Fetch the constraint.
//  2. Skip if the latest advice for it is not yet acknowledged (per the
//     ClusterStateTracker's watch).
//  3. Filter backed-off NodePlacements out of the constraint.
//  4. Build a snapshot copy from r.csTracker.
//  5. Run the planner (embedded, in-process).
//  6. Verify advice against fresh snapshot + constraints.
//  7. Publish ClusterScalingAdvice on success.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.log.WithValues("namespace", req.Namespace, "name", req.Name)
	now := time.Now() // TODO: GLobal time vs. per-step time? Should we pass now to FilterConstraint and Snapshot?

	// Step 1: fetch constraint. If not found, skip without error; watch will trigger on next create.
	constraint := &corev1alpha1.ScalingConstraint{}
	if err := r.client.Get(ctx, req.NamespacedName, constraint); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("ScalingConstraint not found; skipping reconcile")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Step 2: skip if the latest advice has not yet been acknowledged.
	if !r.csTracker.IsLatestAdviceAcknowledged(commontypes.NamespacedName(req.NamespacedName)) {
		log.Info("latest ScalingAdvice not yet acknowledged; skipping reconcile")
		return ctrl.Result{RequeueAfter: r.config.RequeueAfter.Duration}, nil
	}

	// Step 3: drop NodePlacements currently in backoff before planning.
	filteredConstraint := r.csTracker.FilterConstraint(constraint, now)
	if filteredConstraint == nil || len(filteredConstraint.Spec.NodePools) == 0 {
		log.Info("all node pools currently in backoff; nothing to plan")
		return ctrl.Result{RequeueAfter: r.config.RequeueAfter.Duration}, nil
	}

	// Step 4: snapshot copy.
	snap, err := r.csTracker.Snapshot(ctx, now)
	if err != nil {
		if errors.Is(err, statetracker.ErrNotSynced) {
			log.Info("waiting for cluster snapshot to sync; requeueing")
			return ctrl.Result{RequeueAfter: shortRetry}, nil
		}
		return ctrl.Result{}, err
	}
	log.Info("snapshot retrieved",
		"pods", len(snap.Pods),
		"nodes", len(snap.Nodes),
		"upcoming", len(snap.UpcomingNodes),
		"pvs", len(snap.PVs),
		"pvcs", len(snap.PVCs))

	// Step 5: run the embedded planner.
	plan, err := r.runPlanner(ctx, log, snap, filteredConstraint)
	if err != nil {
		switch classifyPlannerErr(err) {
		case plannerErrRetry:
			log.Info("planner produced no advice this tick", "reason", err.Error())
			return ctrl.Result{RequeueAfter: r.config.RequeueAfter.Duration}, nil
		case plannerErrConfig:
			log.Error(err, "planner rejected request as misconfigured")
			return ctrl.Result{RequeueAfter: r.config.RequeueAfter.Duration}, nil
		default:
			return ctrl.Result{}, fmt.Errorf("planner run failed: %w", err)
		}
	}
	if plan == nil || (len(plan.Items) == 0 && len(plan.UnsatisfiedPodNames) == 0) {
		log.Info("planner produced empty plan; skipping publish")
		return ctrl.Result{RequeueAfter: r.config.RequeueAfter.Duration}, nil
	}
	log.Info("planner produced plan",
		"items", len(plan.Items),
		"unsatisfiedPods", len(plan.UnsatisfiedPodNames))

	// Step 6: verify advice.
	// TODO:
	//   a. Re-fetch fresh snapshot via r.csTracker.Snapshot(); compare to planning snapshot. If
	//      delta exceeds threshold, reject.
	//   b. Compare delta/current against node-pool min/max quotas.

	// Step 7: publish ScalingAdvice. Each successful run produces a CR with a
	// fresh UUID-based name; advice is never updated in place.
	if err := r.publishAdvice(ctx, log, constraint, plan); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to publish ScalingAdvice: %w", err)
	}

	return ctrl.Result{RequeueAfter: r.config.RequeueAfter.Duration}, nil
}
