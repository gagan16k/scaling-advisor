// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package reconciler implements a configurable lifecycle-controller stand-in.
// It watches ScalingAdvice, optionally creates/deletes real Nodes on a
// separate data-plane client, and writes ScalingFeedback according to
// per-placement failure rules read from a YAML config.
package reconciler

import (
	"context"
	"fmt"
	"time"

	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/common/nodeutil"
	"github.com/gardener/scaling-advisor/test/fakeactuator/internal/config"
	"github.com/go-logr/logr"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	componentName  = "fake-actuator"
	controllerName = "fake-actuator-controller"
)

// Reconciler watches ScalingAdvice, applies scale-in and scale-out via
// dpClient (data-plane), then writes feedback back via cpClient (control-plane).
type Reconciler struct {
	cpClient client.Client
	dpClient client.Client
	log      logr.Logger
	cfg      *config.Config
}

// NewReconciler creates a Reconciler using the manager's client as the
// control-plane client and dpClient as the data-plane client.
func NewReconciler(mgr ctrl.Manager, dpClient client.Client, cfg *config.Config) *Reconciler {
	return &Reconciler{
		cpClient: mgr.GetClient(),
		dpClient: dpClient,
		log:      mgr.GetLogger().WithName(componentName),
		cfg:      cfg,
	}
}

// NewReconcilerForTest creates a Reconciler with explicit client injection for unit tests.
func NewReconcilerForTest(cpClient, dpClient client.Client, cfg *config.Config) *Reconciler {
	if cfg == nil {
		cfg = config.Default()
	}
	return &Reconciler{
		cpClient: cpClient,
		dpClient: dpClient,
		log:      logr.Discard(),
		cfg:      cfg,
	}
}

// SetupWithManager registers the controller.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return builder.ControllerManagedBy(mgr).
		Named(controllerName).
		For(&corev1alpha1.ScalingAdvice{}).
		Complete(r)
}

// Reconcile processes a single ScalingAdvice.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.log.WithValues("namespace", req.Namespace, "name", req.Name)

	advice := &corev1alpha1.ScalingAdvice{}
	if err := r.cpClient.Get(ctx, req.NamespacedName, advice); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if advice.Status.Feedback != nil {
		log.V(1).Info("advice already acked; skipping")
		return ctrl.Result{}, nil
	}

	constraint := &corev1alpha1.ScalingConstraint{}
	if err := r.cpClient.Get(ctx, types.NamespacedName{
		Namespace: advice.Spec.ConstraintRef.Namespace,
		Name:      advice.Spec.ConstraintRef.Name,
	}, constraint); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("constraint not found; will retry on next event")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	now := time.Now()

	if err := r.applyScaleIn(ctx, advice, log); err != nil {
		return ctrl.Result{}, err
	}

	fb := r.buildFeedback(advice, constraint, now)

	// Write feedback before creating Nodes so the operator always seeds upcoming
	// entries before the real node arrives in the data-plane cache.
	advice.Status.Feedback = fb
	if err := r.cpClient.Status().Update(ctx, advice); err != nil {
		return ctrl.Result{}, fmt.Errorf("update ScalingAdvice status: %w", err)
	}

	if err := r.applyScaleOut(ctx, advice, constraint, fb, now, log); err != nil {
		return ctrl.Result{}, err
	}

	scaleOutItems := 0
	if fb.ScaleOut != nil {
		scaleOutItems = len(fb.ScaleOut.Items)
	}
	scaleInNodes := 0
	if fb.ScaleIn != nil {
		scaleInNodes = len(fb.ScaleIn.AcceptedNodesNames)
	}
	log.Info("acked ScalingAdvice", "scaleOutItems", scaleOutItems, "scaleInNodes", scaleInNodes)
	return ctrl.Result{}, nil
}

// buildFeedback decides per-item ack semantics based on failure rules.
// It does NOT create Nodes — that is done by applyScaleOut after this call.
func (r *Reconciler) buildFeedback(
	advice *corev1alpha1.ScalingAdvice,
	constraint *corev1alpha1.ScalingConstraint,
	now time.Time,
) *corev1alpha1.ScalingFeedback {
	fb := &corev1alpha1.ScalingFeedback{
		ConstraintRef: advice.Spec.ConstraintRef,
	}

	if plan := advice.Spec.ScaleOutPlan; plan != nil && len(plan.Items) > 0 {
		items := make([]corev1alpha1.ScaleOutItemFeedback, 0, len(plan.Items))
		defaultDeadline := metav1.NewTime(now.Add(r.cfg.Timing.CreationDelay))
		for i, item := range plan.Items {
			fbItem := corev1alpha1.ScaleOutItemFeedback{
				Index:            int32(i),
				CreationDeadline: defaultDeadline,
			}

			rule, matched := config.Match(r.cfg.FailureRules, item.NodePlacement)
			// Items where pool/template is not found behave like ResourceExhausted
			// regardless of rules — synthesize a rule so the dispatch below handles
			// them in one place.
			if _, _, ok := nodeutil.FindPoolTemplate(constraint, item.NodePlacement); !ok && !matched {
				rule = config.FailureRule{Mode: config.ModeResourceExhausted}
				matched = true
			}

			if matched {
				backoffDur := r.cfg.Timing.BackoffDuration
				if rule.BackoffDuration > 0 {
					backoffDur = rule.BackoffDuration
				}
				switch rule.Mode {
				case config.ModeResourceExhausted, config.ModeCreationTimeout:
					if rule.Mode == config.ModeResourceExhausted {
						fbItem.ErrorType = corev1alpha1.ScalingErrorTypeResourceExhausted
					} else {
						fbItem.ErrorType = corev1alpha1.ScalingErrorTypeCreationTimeout
					}
					fbItem.FailCount = item.Delta
					if backoffDur > 0 {
						bt := metav1.NewTime(now.Add(backoffDur))
						fbItem.BackoffUntil = &bt
					}

				case config.ModePartial:
					failCount := rule.FailCount
					if failCount <= 0 {
						failCount = 1
					}
					if failCount > item.Delta {
						failCount = item.Delta
					}
					fbItem.FailCount = failCount
					if failCount > 0 && backoffDur > 0 {
						bt := metav1.NewTime(now.Add(backoffDur))
						fbItem.BackoffUntil = &bt
					}

				case config.ModeSilentDeadlineMiss:
					if rule.CreationDelay > 0 {
						fbItem.CreationDeadline = metav1.NewTime(now.Add(rule.CreationDelay))
					}
					// FailCount = Delta is the back-channel signal applyScaleOut reads
					// to skip Node creation. No ErrorType / BackoffUntil — operator
					// prunes via lazy deadline miss.
					fbItem.FailCount = item.Delta
				}
			}

			items = append(items, fbItem)
		}
		fb.ScaleOut = &corev1alpha1.ScaleOutFeedback{Items: items}
	}

	if plan := advice.Spec.ScaleInPlan; plan != nil && len(plan.Items) > 0 {
		names := make([]string, 0, len(plan.Items))
		for _, it := range plan.Items {
			if it.NodeName != "" {
				names = append(names, it.NodeName)
			}
		}
		if len(names) > 0 {
			fb.ScaleIn = &corev1alpha1.ScaleInFeedback{AcceptedNodesNames: names}
		}
	}

	return fb
}

// applyScaleIn deletes the nodes named in the ScaleInPlan with IgnoreNotFound.
func (r *Reconciler) applyScaleIn(ctx context.Context, advice *corev1alpha1.ScalingAdvice, log logr.Logger) error {
	plan := advice.Spec.ScaleInPlan
	if plan == nil || len(plan.Items) == 0 {
		return nil
	}
	if delay := r.cfg.Timing.DeleteGraceDelay; delay > 0 {
		time.Sleep(delay)
	}
	for _, it := range plan.Items {
		if it.NodeName == "" {
			continue
		}
		node := &corev1.Node{}
		node.Name = it.NodeName
		if err := r.dpClient.Delete(ctx, node); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete node %s: %w", it.NodeName, err)
		}
		log.Info("deleted node", "node", it.NodeName)
	}
	return nil
}

// applyScaleOut creates Nodes on the data-plane for items that are not failed
// (ResourceExhausted, CreationTimeout, or SilentDeadlineMiss → 0 nodes;
// Partial → Delta-failCount nodes; success → Delta nodes).
func (r *Reconciler) applyScaleOut(
	ctx context.Context,
	advice *corev1alpha1.ScalingAdvice,
	constraint *corev1alpha1.ScalingConstraint,
	fb *corev1alpha1.ScalingFeedback,
	now time.Time,
	log logr.Logger,
) error {
	if advice.Spec.ScaleOutPlan == nil || fb.ScaleOut == nil {
		return nil
	}
	plan := advice.Spec.ScaleOutPlan

	if delay := r.cfg.Timing.NodeCreateDelay; delay > 0 {
		time.Sleep(delay)
	}

	readyConditions := nodeutil.BuildReadyConditions(now)

	for _, fbItem := range fb.ScaleOut.Items {
		if int(fbItem.Index) >= len(plan.Items) {
			continue
		}
		item := plan.Items[fbItem.Index]

		// Count of nodes to create: ErrorType set → 0 (full failure);
		// FailCount > 0 → Delta-FailCount (partial / silentDeadlineMiss);
		// else Delta (success).
		createCount := item.Delta
		if fbItem.ErrorType == corev1alpha1.ScalingErrorTypeResourceExhausted ||
			fbItem.ErrorType == corev1alpha1.ScalingErrorTypeCreationTimeout {
			createCount = 0
		} else if fbItem.FailCount > 0 {
			createCount = max(item.Delta-fbItem.FailCount, 0)
		}

		if createCount == 0 {
			continue
		}

		pool, template, ok := nodeutil.FindPoolTemplate(constraint, item.NodePlacement)
		if !ok {
			// buildFeedback already synthesized ResourceExhausted for this case
			// (createCount would be 0); defensive guard for safety.
			continue
		}

		asyncFlip := r.cfg.Timing.NodeReadyDelay > 0
		for replicaIdx := int32(0); replicaIdx < createCount; replicaIdx++ {
			ni := nodeutil.BuildUpcomingNodeInfo(advice.UID, fbItem.Index, replicaIdx, pool, template, item.NodePlacement, readyConditions)
			node := nodeutil.AsNode(ni)
			node.ResourceVersion = ""

			if err := r.dpClient.Create(ctx, node); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					return fmt.Errorf("create node %s: %w", node.Name, err)
				}
				// AlreadyExists: re-Get to refresh ResourceVersion for status update.
				existing := &corev1.Node{}
				if err2 := r.dpClient.Get(ctx, types.NamespacedName{Name: node.Name}, existing); err2 != nil {
					return fmt.Errorf("get existing node %s: %w", node.Name, err2)
				}
				node = existing
			}

			conds := ni.Conditions
			if asyncFlip {
				// Set Ready=False now; flip to True asynchronously after delay.
				conds = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
			}
			node.Status = corev1.NodeStatus{
				Capacity:    ni.Capacity,
				Allocatable: ni.Allocatable,
				Conditions:  conds,
			}
			if err := r.dpClient.Status().Update(ctx, node); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("update node status %s: %w", node.Name, err)
			}
			if asyncFlip {
				nodeName := node.Name
				delay := r.cfg.Timing.NodeReadyDelay
				go func() {
					time.Sleep(delay)
					r.flipNodeReady(nodeName, now)
				}()
			}
			log.Info("created node", "node", node.Name, "pool", item.NodePlacement.PoolName, "zone", item.NodePlacement.AvailabilityZone)
			if err := r.ensureNodeLease(ctx, node.Name, now); err != nil {
				return err
			}
			go r.renewNodeLease(node.Name)
		}
	}
	return nil
}

// flipNodeReady performs a best-effort async flip of the node to Ready=True.
func (r *Reconciler) flipNodeReady(nodeName string, transitionTime time.Time) {
	ctx := context.Background()
	existing := &corev1.Node{}
	if err := r.dpClient.Get(ctx, types.NamespacedName{Name: nodeName}, existing); err != nil {
		r.log.V(1).Info("flipNodeReady: get failed", "node", nodeName, "err", err)
		return
	}
	existing.Status.Conditions = nodeutil.BuildReadyConditions(transitionTime)
	if err := r.dpClient.Status().Update(ctx, existing); err != nil {
		r.log.V(1).Info("flipNodeReady: update failed", "node", nodeName, "err", err)
	}
}

// ensureNodeLease creates or updates the node's Lease in kube-node-lease so the
// node-lifecycle controller does not taint the node as unreachable.
func (r *Reconciler) ensureNodeLease(ctx context.Context, nodeName string, now time.Time) error {
	leaseDurationSeconds := int32(40)
	renewTime := metav1.NewMicroTime(now)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeName,
			Namespace: "kube-node-lease",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr.To(nodeName),
			LeaseDurationSeconds: &leaseDurationSeconds,
			RenewTime:            &renewTime,
		},
	}
	if err := r.dpClient.Create(ctx, lease); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create lease for node %s: %w", nodeName, err)
		}
	}
	return nil
}

// renewNodeLease refreshes the node lease every 10 seconds so the node-lifecycle
// controller never marks the fake node as unreachable. Runs until the node is gone.
func (r *Reconciler) renewNodeLease(nodeName string) {
	const interval = 10 * time.Second
	leaseDurationSeconds := int32(40)
	for {
		time.Sleep(interval)
		ctx := context.Background()
		lease := &coordinationv1.Lease{}
		if err := r.dpClient.Get(ctx, types.NamespacedName{Namespace: "kube-node-lease", Name: nodeName}, lease); err != nil {
			if apierrors.IsNotFound(err) {
				return // node lease gone — node was deleted
			}
			r.log.V(1).Info("renewNodeLease: get failed", "node", nodeName, "err", err)
			continue
		}
		renewTime := metav1.NewMicroTime(time.Now())
		lease.Spec.HolderIdentity = ptr.To(nodeName)
		lease.Spec.LeaseDurationSeconds = &leaseDurationSeconds
		lease.Spec.RenewTime = &renewTime
		if err := r.dpClient.Update(ctx, lease); err != nil {
			r.log.V(1).Info("renewNodeLease: update failed", "node", nodeName, "err", err)
		}
	}
}
