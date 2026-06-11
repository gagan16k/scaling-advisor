// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package advisor

import (
	"context"
	"errors"
	"fmt"

	configv1alpha1 "github.com/gardener/scaling-advisor/api/config/v1alpha1"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/operator/internal/controller/clusterstate"
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
	csTracker *clusterstate.ClusterStateTracker
	planner   *plannerStack
}

// NewReconciler eagerly builds the planner stack; failure here aborts manager startup.
func NewReconciler(mgr ctrl.Manager, opCfg *configv1alpha1.OperatorConfig, csBuilder *clusterstate.ClusterStateTracker) (*Reconciler, error) {
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
		// TODO: also watch ScalingFeedback (corev1alpha1.ScalingFeedback) and map back to its
		// constraint, so feedback ack/nack drives a reconcile.
		Complete(r)
}

// Reconcile implements. Its logic follows the steps:
//  1. Fetch the constraint.
//  2. Fetch the latest advice for it; skip / requeue / mark stale per ack state.
//  3. Build a snapshot copy from r.csBuilder.
//  4. Fetch in-memory feedback.
//  5. Filter backed-off / invalid node groups.
//  6. Run the planner (embedded, in-process).
//  7. Verify advice against fresh snapshot + constraints.
//  8. Publish ClusterScalingAdvice on success.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.log.WithValues("namespace", req.Namespace, "name", req.Name)

	constraint := &corev1alpha1.ScalingConstraint{}
	if err := r.client.Get(ctx, req.NamespacedName, constraint); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("ScalingConstraint not found; skipping reconcile")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Step 2: fetch latest advice; honour ack-pending / stale-timeout rules.
	// TODO: list ClusterScalingAdvice for this constraint, pick latest revision.
	//   - If advice is ack-pending and not yet stale: return ctrl.Result{RequeueAfter: ...}.
	//   - If advice is too old (configurable timeout): mark stale, fall through.

	// Step 3: snapshot copy.
	snap, err := r.csTracker.Snapshot()
	if err != nil {
		if errors.Is(err, clusterstate.ErrNotSynced) {
			log.Info("waiting for cluster snapshot to sync; requeueing")
			return ctrl.Result{RequeueAfter: shortRetry}, nil
		}
		return ctrl.Result{}, err
	}
	log.Info("snapshot retrieved",
		"pods", len(snap.Pods),
		"nodes", len(snap.Nodes),
		"pvs", len(snap.PVs),
		"pvcs", len(snap.PVCs))

	// Step 4: feedback lookup.
	// TODO: read from in-memory feedback store keyed by constraint name.

	// Step 5: filter node groups.
	// TODO: build the planner-facing view of node pools by removing those currently in backoff
	//       (per feedback) or otherwise invalid.

	// Step 6: run the embedded planner.
	plan, err := r.runPlanner(ctx, log, snap, constraint)
	if err != nil {
		// Return the error so the workqueue applies rate-limited backoff;
		// watch and heartbeat triggers continue to fire independently.
		return ctrl.Result{}, fmt.Errorf("planner run failed: %w", err)
	}
	if plan == nil || (len(plan.Items) == 0 && len(plan.UnsatisfiedPodNames) == 0) {
		log.Info("planner produced empty plan; skipping publish")
		return ctrl.Result{RequeueAfter: r.config.RequeueAfter.Duration}, nil
	}

	// Step 7: verify advice.
	// TODO:
	//   a. Re-fetch fresh snapshot via r.csTracker.Snapshot(); compare to planning snapshot. If
	//      delta exceeds threshold, reject.
	//   b. Compare delta/current against node-pool min/max quotas.

	// Step 8: publish ScalingAdvice. Each successful run produces a CR with a
	// fresh UUID-based name; advice is never updated in place.
	if err := r.publishAdvice(ctx, log, constraint, plan); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to publish ScalingAdvice: %w", err)
	}

	return ctrl.Result{RequeueAfter: r.config.RequeueAfter.Duration}, nil
}
