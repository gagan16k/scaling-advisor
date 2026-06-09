// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package advisor

import (
	"errors"
	"testing"

	"github.com/gardener/scaling-advisor/api/planner"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"
)

func newTestBuilder() *ClusterSnapshotBuilder {
	b := &ClusterSnapshotBuilder{
		log:   logr.Discard(),
		state: newClusterState(),
	}
	b.synced.Store(true)
	return b
}

func mustSnapshot(t *testing.T, b *ClusterSnapshotBuilder) planner.ClusterSnapshot {
	t.Helper()
	snap, err := b.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	return snap
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

func TestPodAddStored(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	h.AddFunc(unschedulablePod("ns", "pending"))
	h.AddFunc(scheduledPod("ns", "running", "node-a"))

	if got := len(mustSnapshot(t, b).Pods); got != 2 {
		t.Fatalf("snapshot pods = %d, want 2", got)
	}
}

func TestPodUpdateStored(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	old := scheduledPod("ns", "p", "node-a")
	h.AddFunc(old)
	h.UpdateFunc(old, unschedulablePod("ns", "p"))

	if got := len(mustSnapshot(t, b).Pods); got != 1 {
		t.Errorf("snapshot pods after update = %d, want 1", got)
	}
}

func TestPodDelete(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	pod := unschedulablePod("ns", "p")
	h.AddFunc(pod)
	h.DeleteFunc(pod)

	if got := len(mustSnapshot(t, b).Pods); got != 0 {
		t.Errorf("snapshot pods after delete = %d, want 0", got)
	}
}

func TestPodDeleteTombstone(t *testing.T) {
	b := newTestBuilder()
	h := b.podHandler().(toolscache.ResourceEventHandlerFuncs)

	pod := unschedulablePod("ns", "p")
	h.AddFunc(pod)

	tombstone := toolscache.DeletedFinalStateUnknown{Key: "ns/p", Obj: pod}
	h.DeleteFunc(tombstone)

	if got := len(mustSnapshot(t, b).Pods); got != 0 {
		t.Errorf("snapshot pods after tombstone delete = %d, want 0", got)
	}
}

func TestPVUnboundFiltered(t *testing.T) {
	b := newTestBuilder()
	h := b.pvHandler().(toolscache.ResourceEventHandlerFuncs)

	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-unbound"}}
	h.AddFunc(pv)

	if got := len(mustSnapshot(t, b).PVs); got != 0 {
		t.Errorf("expected unbound PV to be filtered, got %d entries", got)
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
	if got := len(mustSnapshot(t, b).PVs); got != 0 {
		t.Fatalf("after unbound add: PVs = %d, want 0", got)
	}

	h.UpdateFunc(unbound, bound)
	if got := len(mustSnapshot(t, b).PVs); got != 1 {
		t.Fatalf("after unbound→bound: PVs = %d, want 1", got)
	}

	// Transition back to unbound — should evict.
	h.UpdateFunc(bound, unbound)
	if got := len(mustSnapshot(t, b).PVs); got != 0 {
		t.Fatalf("after bound→unbound: PVs = %d, want 0", got)
	}
}

func TestNodeAddUpdateDelete(t *testing.T) {
	b := newTestBuilder()
	h := b.nodeHandler().(toolscache.ResourceEventHandlerFuncs)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-a",
			Labels: nodeLabels(),
		},
	}
	h.AddFunc(node)
	if got := len(mustSnapshot(t, b).Nodes); got != 1 {
		t.Fatalf("after add: nodes = %d, want 1", got)
	}

	updated := node.DeepCopy()
	updated.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	h.UpdateFunc(node, updated)
	if got := len(mustSnapshot(t, b).Nodes); got != 1 {
		t.Errorf("after update: nodes = %d, want 1", got)
	}

	h.DeleteFunc(updated)
	if got := len(mustSnapshot(t, b).Nodes); got != 0 {
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

	got := mustSnapshot(t, b).StorageClasses
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
	first := mustSnapshot(t, b)

	// Add another pod after the first Build.
	h.AddFunc(unschedulablePod("ns", "p2"))

	// First snapshot must remain at one element.
	if got := len(first.Pods); got != 1 {
		t.Errorf("first snapshot pod count = %d, want 1 (Build returned aliased state)", got)
	}

	// Second Build observes the new pod.
	if got := len(mustSnapshot(t, b).Pods); got != 2 {
		t.Errorf("second snapshot pod count = %d, want 2", got)
	}
}

func TestSnapshotNotSyncedReturnsError(t *testing.T) {
	b := &ClusterSnapshotBuilder{
		log:   logr.Discard(),
		state: newClusterState(),
	}
	if _, err := b.Snapshot(); !errors.Is(err, ErrSnapshotNotSynced) {
		t.Errorf("Snapshot() error = %v, want ErrSnapshotNotSynced", err)
	}
}
