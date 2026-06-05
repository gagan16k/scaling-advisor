// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package advisor

import (
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// newTestBuilder returns a builder with no cache wired — suitable for driving the per-type
// handler factories directly without controller-runtime.
func newTestBuilder() *ClusterSnapshotBuilder {
	return &ClusterSnapshotBuilder{
		log:     logr.Discard(),
		state:   newClusterState(),
		trigger: make(chan event.GenericEvent, 1),
	}
}

func drainTrigger(b *ClusterSnapshotBuilder) bool {
	select {
	case <-b.trigger:
		return true
	default:
		return false
	}
}

func unschedulablePod(namespace, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionFalse},
			},
		},
	}
}

func scheduledPod(namespace, name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			},
		},
	}
}

func TestPodAddUnschedulableSignals(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	h.AddFunc(unschedulablePod("ns", "pending"))

	if got := len(b.Build().Pods); got != 1 {
		t.Fatalf("snapshot pods = %d, want 1", got)
	}
	if !drainTrigger(b) {
		t.Errorf("expected trigger to fire on add of unschedulable pod")
	}
}

func TestPodAddScheduledNoSignal(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	h.AddFunc(scheduledPod("ns", "running", "node-a"))

	if got := len(b.Build().Pods); got != 1 {
		t.Fatalf("snapshot pods = %d, want 1", got)
	}
	if drainTrigger(b) {
		t.Errorf("did not expect trigger to fire on add of scheduled pod")
	}
}

func TestPodTransitionToUnschedulableSignals(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	old := scheduledPod("ns", "p", "node-a")
	h.AddFunc(old)
	_ = drainTrigger(b) // ignore — Add of scheduled does not signal anyway

	updated := unschedulablePod("ns", "p")
	h.UpdateFunc(old, updated)

	if !drainTrigger(b) {
		t.Errorf("expected trigger on transition to unschedulable")
	}
}

func TestPodTransitionOutOfUnschedulableSignals(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	old := unschedulablePod("ns", "p")
	h.AddFunc(old)
	if !drainTrigger(b) {
		t.Fatal("setup: expected initial trigger on Add of unschedulable")
	}

	updated := scheduledPod("ns", "p", "node-a")
	h.UpdateFunc(old, updated)

	if !drainTrigger(b) {
		t.Errorf("expected trigger on transition out of unschedulable")
	}
}

func TestPodDeleteOfUnschedulableSignals(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	pod := unschedulablePod("ns", "p")
	h.AddFunc(pod)
	_ = drainTrigger(b)

	h.DeleteFunc(pod)

	if !drainTrigger(b) {
		t.Errorf("expected trigger on delete of unschedulable pod")
	}
	if got := len(b.Build().Pods); got != 0 {
		t.Errorf("snapshot pods after delete = %d, want 0", got)
	}
}

func TestPodDeleteScheduledNoSignal(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	pod := scheduledPod("ns", "p", "node-a")
	h.AddFunc(pod)
	_ = drainTrigger(b)

	h.DeleteFunc(pod)

	if drainTrigger(b) {
		t.Errorf("did not expect trigger on delete of already-scheduled pod")
	}
}

func TestPodDeleteTombstone(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	pod := unschedulablePod("ns", "p")
	h.AddFunc(pod)
	_ = drainTrigger(b)

	tombstone := toolscache.DeletedFinalStateUnknown{Key: "ns/p", Obj: pod}
	h.DeleteFunc(tombstone)

	if !drainTrigger(b) {
		t.Errorf("expected trigger via tombstone delete")
	}
	if got := len(b.Build().Pods); got != 0 {
		t.Errorf("snapshot pods after tombstone delete = %d, want 0", got)
	}
}

func TestPVUnboundFiltered(t *testing.T) {
	b := newTestBuilder()
	h := b.pvHandler().(toolscache.ResourceEventHandlerFuncs)

	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-unbound"}}
	h.AddFunc(pv)

	if got := len(b.Build().PVs); got != 0 {
		t.Errorf("expected unbound PV to be filtered, got %d entries", got)
	}
	if drainTrigger(b) {
		t.Errorf("did not expect trigger on add of unbound PV")
	}
}

func TestPVUnboundToBoundAndBack(t *testing.T) {
	b := newTestBuilder()
	h := b.pvHandler().(toolscache.ResourceEventHandlerFuncs)

	unbound := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
		// AsPVInfo dereferences Spec.NodeAffinity unconditionally; provide an empty struct.
		Spec: corev1.PersistentVolumeSpec{NodeAffinity: &corev1.VolumeNodeAffinity{}},
	}
	bound := unbound.DeepCopy()
	bound.Spec.ClaimRef = &corev1.ObjectReference{Namespace: "ns", Name: "claim"}

	h.AddFunc(unbound)
	if got := len(b.Build().PVs); got != 0 {
		t.Fatalf("after unbound add: PVs = %d, want 0", got)
	}

	h.UpdateFunc(unbound, bound)
	if got := len(b.Build().PVs); got != 1 {
		t.Fatalf("after unbound→bound: PVs = %d, want 1", got)
	}
	if !drainTrigger(b) {
		t.Errorf("expected trigger on PV becoming bound")
	}

	// Transition back to unbound — should evict.
	h.UpdateFunc(bound, unbound)
	if got := len(b.Build().PVs); got != 0 {
		t.Fatalf("after bound→unbound: PVs = %d, want 0", got)
	}
	if !drainTrigger(b) {
		t.Errorf("expected trigger on PV unbinding")
	}
}

func TestNodeAddSignalsUpdateDoesNot(t *testing.T) {
	b := newTestBuilder()
	h := b.nodeHandler().(toolscache.ResourceEventHandlerFuncs)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-a",
			Labels: nodeLabels(),
		},
	}
	h.AddFunc(node)
	if !drainTrigger(b) {
		t.Errorf("expected trigger on node add")
	}

	updated := node.DeepCopy()
	updated.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	h.UpdateFunc(node, updated)
	if drainTrigger(b) {
		t.Errorf("did not expect trigger on node update (kubelet status churn)")
	}

	h.DeleteFunc(updated)
	if !drainTrigger(b) {
		t.Errorf("expected trigger on node delete")
	}
	if got := len(b.Build().Nodes); got != 0 {
		t.Errorf("snapshot nodes after delete = %d, want 0", got)
	}
}

// nodeLabels returns the minimum labels required by planner.NodeInfo.ValidateLabels. Empty values
// are fine — only presence matters here.
func nodeLabels() map[string]string {
	return map[string]string{
		"worker.gardener.cloud/pool":     "",
		"worker.gardener.cloud/template": "",
		corev1.LabelTopologyRegion:       "",
		corev1.LabelTopologyZone:         "",
	}
}

func TestStorageClassDeepCopiedOnInsert(t *testing.T) {
	b := newTestBuilder()
	h := b.storageClassHandler().(toolscache.ResourceEventHandlerFuncs)

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "sc-1", Labels: map[string]string{"k": "v1"}},
		Provisioner: "p",
	}
	h.AddFunc(sc)

	// Mutate the source object — the builder's stored copy must be unaffected.
	sc.Labels["k"] = "v2"
	sc.Provisioner = "p-changed"

	got := b.Build().StorageClasses
	if len(got) != 1 {
		t.Fatalf("storageClasses = %d, want 1", len(got))
	}
	if got[0].Provisioner != "p" {
		t.Errorf("Provisioner = %q, want %q (DeepCopy on insert violated)", got[0].Provisioner, "p")
	}
	if got[0].Labels["k"] != "v1" {
		t.Errorf("Labels[k] = %q, want %q", got[0].Labels["k"], "v1")
	}
}

func TestBuildIndependence(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	h.AddFunc(unschedulablePod("ns", "p1"))
	first := b.Build()

	// Add another pod after the first Build.
	h.AddFunc(unschedulablePod("ns", "p2"))

	// First snapshot must remain at one element.
	if got := len(first.Pods); got != 1 {
		t.Errorf("first snapshot pod count = %d, want 1 (Build returned aliased state)", got)
	}

	// Second Build observes the new pod.
	if got := len(b.Build().Pods); got != 2 {
		t.Errorf("second snapshot pod count = %d, want 2", got)
	}
}
