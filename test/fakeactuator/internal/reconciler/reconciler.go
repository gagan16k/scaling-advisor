// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package reconciler implements a minimal lifecycle-controller stand-in:
// it watches ScalingAdvice and writes an ack-shaped ScalingFeedback into
// ScalingAdvice.Status.Feedback so the operator's feedback-aware paths can
// be exercised end-to-end without a real lcc (e.g. MCM).

package reconciler

import (
	"context"
	"fmt"
	"time"

	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	componentName  = "fake-actuator"
	controllerName = "fake-actuator-controller"
)

// Options holds tuning knobs for the actuator.
type Options struct {
	// CreationDelay is added to time.Now() to compute the per-item
	// CreationDeadline written into ScaleOutItemFeedback.
	CreationDelay time.Duration
}

// Reconciler watches ScalingAdvice and writes ack feedback into its status.
type Reconciler struct {
	client client.Client
	log    logr.Logger
	opts   Options
}

// NewReconciler returns a Reconciler bound to mgr's client and logger.
func NewReconciler(mgr ctrl.Manager, opts Options) *Reconciler {
	return &Reconciler{
		client: mgr.GetClient(),
		log:    mgr.GetLogger().WithName(componentName),
		opts:   opts,
	}
}

// SetupWithManager registers the controller
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return builder.ControllerManagedBy(mgr).
		Named(controllerName).
		For(&corev1alpha1.ScalingAdvice{}).
		Complete(r)
}

// Reconcile acks a single ScalingAdvice. If the advice is already acked (Status.Feedback non-nil), skipped
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.log.WithValues("namespace", req.Namespace, "name", req.Name)

	advice := &corev1alpha1.ScalingAdvice{}
	if err := r.client.Get(ctx, req.NamespacedName, advice); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if advice.Status.Feedback != nil {
		log.V(1).Info("advice already acked; skipping")
		return ctrl.Result{}, nil
	}

	fb := buildAck(advice, time.Now().Add(r.opts.CreationDelay))
	advice.Status.Feedback = fb
	if err := r.client.Status().Update(ctx, advice); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update ScalingAdvice status: %w", err)
	}

	scaleOutItems := 0
	if fb.ScaleOut != nil {
		scaleOutItems = len(fb.ScaleOut.Items)
	}
	scaleInNodes := 0
	if fb.ScaleIn != nil {
		scaleInNodes = len(fb.ScaleIn.AcceptedNodesNames)
	}
	log.Info("acked ScalingAdvice",
		"scaleOutItems", scaleOutItems,
		"scaleInNodes", scaleInNodes)
	return ctrl.Result{}, nil
}

// buildAck produces a ScalingFeedback for advice.
func buildAck(advice *corev1alpha1.ScalingAdvice, deadline time.Time) *corev1alpha1.ScalingFeedback {
	fb := &corev1alpha1.ScalingFeedback{
		ConstraintRef: advice.Spec.ConstraintRef,
	}

	if plan := advice.Spec.ScaleOutPlan; plan != nil && len(plan.Items) > 0 {
		items := make([]corev1alpha1.ScaleOutItemFeedback, len(plan.Items))
		dl := metav1.NewTime(deadline)
		for i := range plan.Items {
			items[i] = corev1alpha1.ScaleOutItemFeedback{
				Index:            int32(i),
				CreationDeadline: dl,
			}
		}
		fb.ScaleOut = &corev1alpha1.ScaleOutFeedback{Items: items}
	}

	if plan := advice.Spec.ScaleInPlan; plan != nil && len(plan.Items) > 0 {
		names := make([]string, 0, len(plan.Items))
		for _, it := range plan.Items {
			if it.NodeName == "" {
				continue
			}
			names = append(names, it.NodeName)
		}
		if len(names) > 0 {
			fb.ScaleIn = &corev1alpha1.ScaleInFeedback{AcceptedNodesNames: names}
		}
	}

	return fb
}
