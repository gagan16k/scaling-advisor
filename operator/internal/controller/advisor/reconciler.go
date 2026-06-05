// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package advisor

import (
	"context"

	configv1alpha1 "github.com/gardener/scaling-advisor/api/config/v1alpha1"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	componentName  = "operator-reconciler"
	controllerName = "scaling-advisor-controller"
)

// Reconciler reconciles ScalingConstraint against the latest in-memory ClusterSnapshot and
// (eventually) drives the embedded planner to produce ScalingAdvice. See docs/operator.md.
type Reconciler struct {
	client    client.Client
	log       logr.Logger
	config    configv1alpha1.ScalingAdviceControllerConfig
	csBuilder *ClusterSnapshotBuilder
}

// NewReconciler returns a Reconciler bound to the given manager and snapshot builder.
func NewReconciler(mgr ctrl.Manager, config configv1alpha1.ScalingAdviceControllerConfig, csBuilder *ClusterSnapshotBuilder) *Reconciler {
	return &Reconciler{
		client:    mgr.GetClient(),
		log:       mgr.GetLogger().WithName(componentName),
		config:    config,
		csBuilder: csBuilder,
	}
}

// SetupWithManager wires the reconciler. Reconcile fires on the snapshot builder's trigger
// (meaningful cluster-state changes) or the RequeueAfter heartbeat. Once the builder reports
// Ready, one Notify() guarantees a first reconcile on a quiet cluster.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	mapToTick := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, _ client.Object) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "scaling-advisor", Name: "tick"}}}
	})
	err := builder.ControllerManagedBy(mgr).
		Named(controllerName).
		WatchesRawSource(source.Channel(r.csBuilder.Trigger(), mapToTick)).
		// For(&corev1alpha1.ScalingConstraint{}).
		// TODO: also watch ScalingFeedback (corev1alpha1.ScalingFeedback) and map back to its
		// constraint, so feedback ack/nack drives a reconcile.
		Complete(r)
	if err != nil {
		return err
	}
	// Kick the controller once after initial sync so a quiet cluster gets a first reconcile
	// without waiting for the heartbeat.
	return mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		select {
		case <-r.csBuilder.Ready():
		case <-ctx.Done():
			return nil
		}
		r.csBuilder.Notify()
		return nil
	}))
}

// Reconcile implements the pseudocode in docs/operator.md:
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
	dummyNsName := types.NamespacedName{Namespace: "default", Name: "example"}
	if err := r.client.Get(ctx, dummyNsName, constraint); err != nil {
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
	snap := r.csBuilder.Build()
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
		// Intentionally non-fatal: a returned error would break the watch-trigger
		// semantics set up in SetupWithManager. The next tick retries naturally.
		log.Error(err, "planner failed; will retry on next tick")
		return ctrl.Result{RequeueAfter: r.config.RequeueAfter.Duration}, nil
	}
	if plan == nil || (len(plan.Items) == 0 && len(plan.UnsatisfiedPodNames) == 0) {
		log.V(1).Info("planner produced empty plan; skipping publish")
		return ctrl.Result{RequeueAfter: r.config.RequeueAfter.Duration}, nil
	}

	// Step 7: verify advice.
	// TODO:
	//   a. Re-fetch fresh snapshot via r.csBuilder.Build(); compare to planning snapshot. If
	//      delta exceeds threshold, reject.
	//   b. Compare delta/current against node-pool min/max quotas.

	// Step 8: publish ScalingAdvice (idempotent by snapshot hash; never updated in place).
	if err := r.publishAdvice(ctx, log, constraint, plan, snap); err != nil {
		log.Error(err, "failed to publish ScalingAdvice; will retry on next tick")
	}

	return ctrl.Result{RequeueAfter: r.config.RequeueAfter.Duration}, nil
}
