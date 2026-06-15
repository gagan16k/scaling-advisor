// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statetracker

import (
	"reflect"
	"testing"
	"time"

	commonconstants "github.com/gardener/scaling-advisor/api/common/constants"
	apicommon "github.com/gardener/scaling-advisor/api/common/types"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// testConstraint builds a constraint with one pool, one template, two zones.
func testConstraint() *corev1alpha1.ScalingConstraint {
	return &corev1alpha1.ScalingConstraint{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c1"},
		Spec: corev1alpha1.ScalingConstraintSpec{
			NodePools: []corev1alpha1.NodePool{{
				Name:              "pool-a",
				Region:            "r1",
				AvailabilityZones: []string{"zone-a", "zone-b"},
				Labels:            map[string]string{"team": "x"},
				Annotations:       map[string]string{"note": "y"},
				Taints:            []corev1.Taint{{Key: "k", Value: "v", Effect: corev1.TaintEffectNoSchedule}},
				NodeTemplates: []corev1alpha1.NodeTemplate{{
					Name:         "tmpl-1",
					InstanceType: "m5.large",
					Architecture: "amd64",
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("8"),
						corev1.ResourceMemory: resource.MustParse("32Gi"),
					},
					KubeReserved: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("1"),
					},
					SystemReserved: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
				}},
			}},
		},
	}
}

// testAdvice builds an acknowledged advice for the given pool/template/zone with a given Delta.
func testAdvice(uid string, deadline time.Time, backoffUntil *time.Time, delta int32, zone string) *corev1alpha1.ScalingAdvice {
	var backoff *metav1.Time
	if backoffUntil != nil {
		backoff = &metav1.Time{Time: *backoffUntil}
	}
	return &corev1alpha1.ScalingAdvice{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "a-" + uid, UID: types.UID(uid)},
		Spec: corev1alpha1.ScalingAdviceSpec{
			ConstraintRef: apicommon.NamespacedName{Namespace: "ns", Name: "c1"},
			ScaleOutPlan: &corev1alpha1.ScaleOutPlan{
				Items: []corev1alpha1.ScaleOutItem{{
					NodePlacement: corev1alpha1.NodePlacement{
						PoolName: "pool-a", TemplateName: "tmpl-1",
						InstanceType: "m5.large", Region: "r1", AvailabilityZone: zone,
					},
					Delta: delta,
				}},
			},
		},
		Status: corev1alpha1.ScalingAdviceStatus{
			Feedback: &corev1alpha1.ScalingFeedback{
				ScaleOut: &corev1alpha1.ScaleOutFeedback{
					Items: []corev1alpha1.ScaleOutItemFeedback{{
						Index:            0,
						CreationDeadline: metav1.Time{Time: deadline},
						BackoffUntil:     backoff,
					}},
				},
			},
		},
	}
}

func TestSynthesizeUpcomingNodes_CountAndLabels(t *testing.T) {
	deadline := time.Now().Add(5 * time.Minute)
	advice := testAdvice("u1", deadline, nil, 3, "zone-a")
	c := testConstraint()
	got := synthesizeUpcomingNodes(advice, c, time.Now())

	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3", len(got))
	}
	for _, u := range got {
		if u.key.adviceUID != "u1" || u.key.itemIndex != 0 {
			t.Errorf("unexpected key %+v", u.key)
		}
		if u.creationDeadline != deadline {
			t.Errorf("creationDeadline = %v want %v", u.creationDeadline, deadline)
		}
		p, err := u.nodeInfo.GetNodePlacement()
		if err != nil {
			t.Fatalf("GetNodePlacement: %v", err)
		}
		want := corev1alpha1.NodePlacement{
			PoolName: "pool-a", TemplateName: "tmpl-1",
			InstanceType: "m5.large", Region: "r1", AvailabilityZone: "zone-a",
		}
		if p != want {
			t.Errorf("placement = %+v, want %+v", p, want)
		}
		if u.nodeInfo.Labels[commonconstants.LabelNodePoolName] != "pool-a" {
			t.Errorf("missing pool-name label")
		}
		if u.nodeInfo.Labels["team"] != "x" {
			t.Errorf("pool labels not merged")
		}
	}
}

func TestSynthesizeUpcomingNodes_AllocatableSubtract(t *testing.T) {
	advice := testAdvice("u1", time.Now().Add(time.Minute), nil, 1, "zone-a")
	got := synthesizeUpcomingNodes(advice, testConstraint(), time.Now())
	if len(got) != 1 {
		t.Fatalf("len=%d want 1", len(got))
	}
	ni := got[0].nodeInfo
	cpu := ni.Allocatable[corev1.ResourceCPU]
	mem := ni.Allocatable[corev1.ResourceMemory]
	pods := ni.Allocatable[corev1.ResourcePods]
	if cpu.Cmp(resource.MustParse("7")) != 0 {
		t.Errorf("cpu allocatable = %v, want 7", cpu.String())
	}
	if mem.Cmp(resource.MustParse("30Gi")) != 0 {
		t.Errorf("memory allocatable = %v, want 30Gi", mem.String())
	}
	if pods.Cmp(resource.MustParse("110")) != 0 {
		t.Errorf("pods default = %v, want 110", pods.String())
	}
}

func TestSynthesizeUpcomingNodes_UnknownPoolSkipped(t *testing.T) {
	advice := testAdvice("u1", time.Now().Add(time.Minute), nil, 2, "zone-a")
	advice.Spec.ScaleOutPlan.Items[0].PoolName = "pool-missing"
	got := synthesizeUpcomingNodes(advice, testConstraint(), time.Now())
	if len(got) != 0 {
		t.Fatalf("len=%d, want 0 (unknown pool must be skipped)", len(got))
	}
}

func TestExtractBackoffEntries_FutureOnly(t *testing.T) {
	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(5 * time.Minute)
	advice := testAdvice("u1", time.Now().Add(time.Minute), &future, 1, "zone-a")
	// Add a second item with past backoff.
	advice.Spec.ScaleOutPlan.Items = append(advice.Spec.ScaleOutPlan.Items, corev1alpha1.ScaleOutItem{
		NodePlacement: corev1alpha1.NodePlacement{
			PoolName: "pool-a", TemplateName: "tmpl-1",
			InstanceType: "m5.large", Region: "r1", AvailabilityZone: "zone-b",
		},
		Delta: 1,
	})
	advice.Status.Feedback.ScaleOut.Items = append(advice.Status.Feedback.ScaleOut.Items,
		corev1alpha1.ScaleOutItemFeedback{
			Index:            1,
			CreationDeadline: metav1.Time{Time: time.Now().Add(time.Minute)},
			BackoffUntil:     &metav1.Time{Time: past},
		})

	got := extractBackoffEntries(advice, time.Now())
	if len(got) != 1 {
		t.Fatalf("len=%d want 1", len(got))
	}
	want := corev1alpha1.NodePlacement{
		PoolName: "pool-a", TemplateName: "tmpl-1",
		InstanceType: "m5.large", Region: "r1", AvailabilityZone: "zone-a",
	}
	if _, ok := got[want]; !ok {
		t.Fatalf("future backoff entry missing; got %+v", got)
	}
}

func TestFilterConstraintByBackoff_DropsOnlyMatchingZone(t *testing.T) {
	c := testConstraint()
	now := time.Now()
	until := now.Add(5 * time.Minute)
	backoffs := map[corev1alpha1.NodePlacement]time.Time{
		{PoolName: "pool-a", TemplateName: "tmpl-1", InstanceType: "m5.large", Region: "r1", AvailabilityZone: "zone-a"}: until,
	}
	out := filterConstraintByBackoff(c, backoffs, now)

	if len(out.Spec.NodePools) != 1 {
		t.Fatalf("pools=%d, want 1", len(out.Spec.NodePools))
	}
	zones := out.Spec.NodePools[0].AvailabilityZones
	if len(zones) != 1 || zones[0] != "zone-b" {
		t.Fatalf("zones=%v, want [zone-b]", zones)
	}
	if len(out.Spec.NodePools[0].NodeTemplates) != 1 {
		t.Fatalf("templates=%d, want 1", len(out.Spec.NodePools[0].NodeTemplates))
	}
}

func TestFilterConstraintByBackoff_DropsEmptyPool(t *testing.T) {
	c := testConstraint()
	now := time.Now()
	until := now.Add(5 * time.Minute)
	// Backoff every (template, zone) of the pool.
	backoffs := map[corev1alpha1.NodePlacement]time.Time{}
	for _, p := range c.Spec.NodePools[0].GetNodePlacements() {
		backoffs[p] = until
	}
	out := filterConstraintByBackoff(c, backoffs, now)
	if len(out.Spec.NodePools) != 0 {
		t.Fatalf("pools=%d, want 0", len(out.Spec.NodePools))
	}
}

func TestFilterConstraintByBackoff_NonMatchingSurvives(t *testing.T) {
	c := testConstraint()
	now := time.Now()
	backoffs := map[corev1alpha1.NodePlacement]time.Time{
		{PoolName: "other-pool", TemplateName: "tmpl-1", InstanceType: "m5.large", Region: "r1", AvailabilityZone: "zone-a"}: now.Add(5 * time.Minute),
	}
	out := filterConstraintByBackoff(c, backoffs, now)
	if !reflect.DeepEqual(out.Spec.NodePools, c.Spec.NodePools) {
		t.Fatalf("non-matching backoff altered the constraint")
	}
}

func TestFilterConstraintByBackoff_ExpiredBackoffIgnored(t *testing.T) {
	c := testConstraint()
	now := time.Now()
	past := now.Add(-time.Minute)
	backoffs := map[corev1alpha1.NodePlacement]time.Time{
		{PoolName: "pool-a", TemplateName: "tmpl-1", InstanceType: "m5.large", Region: "r1", AvailabilityZone: "zone-a"}: past,
	}
	out := filterConstraintByBackoff(c, backoffs, now)
	zones := out.Spec.NodePools[0].AvailabilityZones
	if len(zones) != 2 {
		t.Fatalf("zones=%v, want both kept (expired backoff)", zones)
	}
}

// TestIsAdviceAcknowledged covers the unacked predicates (nil feedback, nil
// ScaleOut, empty items, any zero CreationDeadline) and the fully-acked path.
func TestIsAdviceAcknowledged(t *testing.T) {
	deadline := time.Now().Add(time.Minute)

	withFeedback := func(fb *corev1alpha1.ScalingFeedback) *corev1alpha1.ScalingAdvice {
		return &corev1alpha1.ScalingAdvice{Status: corev1alpha1.ScalingAdviceStatus{Feedback: fb}}
	}

	tests := map[string]struct {
		advice *corev1alpha1.ScalingAdvice
		want   bool
	}{
		"nil feedback": {
			advice: withFeedback(nil),
			want:   false,
		},
		"nil ScaleOut": {
			advice: withFeedback(&corev1alpha1.ScalingFeedback{}),
			want:   false,
		},
		"empty items": {
			advice: withFeedback(&corev1alpha1.ScalingFeedback{ScaleOut: &corev1alpha1.ScaleOutFeedback{}}),
			want:   false,
		},
		"one item missing CreationDeadline": {
			advice: withFeedback(&corev1alpha1.ScalingFeedback{
				ScaleOut: &corev1alpha1.ScaleOutFeedback{Items: []corev1alpha1.ScaleOutItemFeedback{
					{Index: 0, CreationDeadline: metav1.Time{Time: deadline}},
					{Index: 1}, // zero CreationDeadline
				}},
			}),
			want: false,
		},
		"all items acknowledged": {
			advice: testAdvice("u1", deadline, nil, 1, "zone-a"),
			want:   true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := isAdviceAcknowledged(tc.advice); got != tc.want {
				t.Errorf("isAdviceAcknowledged = %v, want %v", got, tc.want)
			}
		})
	}
}
