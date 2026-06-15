// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statetracker

import (
	"slices"
	"time"

	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/api/planner"
	"github.com/gardener/scaling-advisor/common/nodeutil"
	"k8s.io/apimachinery/pkg/types"
)

// upcomingKey is stable across re-acks of an advice and uniquely keys
// synthetic upcoming nodes drawn from possibly many advices.
type upcomingKey struct {
	adviceUID    types.UID
	itemIndex    int32
	replicaIndex int32
}

// upcomingNode is one synthetic NodeInfo waiting for its real counterpart;
// dropped on matching real-node arrival or when creationDeadline expires.
type upcomingNode struct {
	key              upcomingKey
	nodeInfo         planner.NodeInfo
	placement        corev1alpha1.NodePlacement
	creationDeadline time.Time
}

// isAdviceAcknowledged reports whether the lifecycle manager has acknowledged the advice
// (ScaleOut feedback present and every item carries a non-zero CreationDeadline).
func isAdviceAcknowledged(advice *corev1alpha1.ScalingAdvice) bool {
	fb := advice.Status.Feedback
	if fb == nil || fb.ScaleOut == nil || len(fb.ScaleOut.Items) == 0 {
		return false
	}
	for _, item := range fb.ScaleOut.Items {
		if item.CreationDeadline.IsZero() {
			return false
		}
	}
	return true
}

// extractBackoffEntries returns placements whose feedback carries a future BackoffUntil.
func extractBackoffEntries(advice *corev1alpha1.ScalingAdvice, now time.Time) map[corev1alpha1.NodePlacement]time.Time {
	out := map[corev1alpha1.NodePlacement]time.Time{}
	if advice == nil || advice.Spec.ScaleOutPlan == nil ||
		advice.Status.Feedback == nil || advice.Status.Feedback.ScaleOut == nil {
		return out
	}
	plan := advice.Spec.ScaleOutPlan
	for _, fb := range advice.Status.Feedback.ScaleOut.Items {
		if fb.BackoffUntil == nil || !fb.BackoffUntil.After(now) {
			continue
		}
		if fb.Index < 0 || int(fb.Index) >= len(plan.Items) {
			continue
		}
		out[plan.Items[fb.Index].NodePlacement] = fb.BackoffUntil.Time
	}
	return out
}

// synthesizeUpcomingNodes builds one entry per replica in each acknowledged ScaleOutItem.Delta.
// Items whose pool/templates are no longer in the constraint are skipped.
func synthesizeUpcomingNodes(
	advice *corev1alpha1.ScalingAdvice,
	constraint *corev1alpha1.ScalingConstraint,
	now time.Time,
) []upcomingNode {
	var out []upcomingNode
	if advice == nil || advice.Spec.ScaleOutPlan == nil ||
		advice.Status.Feedback == nil || advice.Status.Feedback.ScaleOut == nil {
		return out
	}
	plan := advice.Spec.ScaleOutPlan
	readyConditions := nodeutil.BuildReadyConditions(now)
	for _, fb := range advice.Status.Feedback.ScaleOut.Items {
		if fb.Index < 0 || int(fb.Index) >= len(plan.Items) {
			continue
		}
		item := plan.Items[fb.Index]
		pool, template, ok := nodeutil.FindPoolTemplate(constraint, item.NodePlacement)
		if !ok {
			continue
		}
		for replicaIdx := int32(0); replicaIdx < item.Delta; replicaIdx++ {
			ni := nodeutil.BuildUpcomingNodeInfo(advice.UID, fb.Index, replicaIdx, pool, template, item.NodePlacement, readyConditions)
			out = append(out, upcomingNode{
				key:              upcomingKey{advice.UID, fb.Index, replicaIdx},
				nodeInfo:         ni,
				placement:        item.NodePlacement,
				creationDeadline: fb.CreationDeadline.Time,
			})
		}
	}
	return out
}

// filterConstraintByBackoff returns a deep-copy of in with every (template, zone) tuple in active backoff removed.
// TODO: Change needed after PR#168
func filterConstraintByBackoff(
	in *corev1alpha1.ScalingConstraint,
	backoffs map[corev1alpha1.NodePlacement]time.Time,
	now time.Time,
) *corev1alpha1.ScalingConstraint {
	if in == nil {
		return nil
	}
	if len(backoffs) == 0 {
		return in
	}
	out := in.DeepCopy()

	survivingPools := make([]corev1alpha1.NodePool, 0, len(out.Spec.NodePools))
	for _, pool := range out.Spec.NodePools {
		type templateSurvivors struct {
			template corev1alpha1.NodeTemplate
			zones    []string
		}
		survivors := make([]templateSurvivors, 0, len(pool.NodeTemplates))
		var firstZones []string
		uniformZones := true
		for _, t := range pool.NodeTemplates {
			zones := make([]string, 0, len(pool.AvailabilityZones))
			for _, az := range pool.AvailabilityZones {
				p := corev1alpha1.NodePlacement{
					PoolName:         pool.Name,
					TemplateName:     t.Name,
					InstanceType:     t.InstanceType,
					Region:           pool.Region,
					AvailabilityZone: az,
				}
				if until, ok := backoffs[p]; ok && until.After(now) {
					continue
				}
				zones = append(zones, az)
			}
			if len(zones) == 0 {
				continue
			}
			survivors = append(survivors, templateSurvivors{template: t, zones: zones})
			// Track whether every surviving template ended up with the same
			// zone set; if so, we can keep one pool entry. Otherwise we must
			// split to avoid resurrecting backed-off (template, zone) pairs
			if firstZones == nil {
				firstZones = zones
			} else if !slices.Equal(zones, firstZones) {
				uniformZones = false
			}
		}
		if len(survivors) == 0 {
			continue
		}
		if uniformZones {
			templates := make([]corev1alpha1.NodeTemplate, 0, len(survivors))
			for _, s := range survivors {
				templates = append(templates, s.template)
			}
			pool.NodeTemplates = templates
			pool.AvailabilityZones = survivors[0].zones
			survivingPools = append(survivingPools, pool)
			continue
		}
		// Split: one pseudo-pool per surviving template. Pool name is preserved
		// so downstream NodePlacement.PoolName still resolves to the original
		// pool; metadata (labels, taints, etc.) is shared via the deep-copied
		// pool above.
		for _, s := range survivors {
			split := pool
			split.NodeTemplates = []corev1alpha1.NodeTemplate{s.template}
			split.AvailabilityZones = s.zones
			survivingPools = append(survivingPools, split)
		}
	}
	out.Spec.NodePools = survivingPools
	return out
}
