// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statetracker

import (
	"fmt"
	"sync"
	"testing"
	"time"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/api/planner"
	nodev1 "k8s.io/api/node/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// These tests exercise clusterState directly, independent of any informer/handler plumbing.
// Handler-level coverage lives in statetracker_test.go.

func podInfo(namespace, name string) planner.PodInfo {
	return planner.PodInfo{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

func nodeInfo(name string) planner.NodeInfo {
	return planner.NodeInfo{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func TestClusterStateUpsertDeletePod(t *testing.T) {
	s := newClusterState()
	key := commontypes.NamespacedName{Namespace: "ns", Name: "p"}

	s.upsertPod(podInfo("ns", "p"))
	if got := len(s.snapshot(time.Now()).Pods); got != 1 {
		t.Fatalf("after upsert: pods = %d, want 1", got)
	}

	// Upsert with same key replaces, doesn't grow the map.
	s.upsertPod(podInfo("ns", "p"))
	if got := len(s.snapshot(time.Now()).Pods); got != 1 {
		t.Fatalf("after second upsert: pods = %d, want 1", got)
	}

	s.deletePod(key)
	if got := len(s.snapshot(time.Now()).Pods); got != 0 {
		t.Fatalf("after delete: pods = %d, want 0", got)
	}
}

func TestClusterStateUpsertDeleteNode(t *testing.T) {
	s := newClusterState()
	s.upsertNode(nodeInfo("node-a"))
	s.upsertNode(nodeInfo("node-b"))
	if got := len(s.snapshot(time.Now()).Nodes); got != 2 {
		t.Fatalf("after two upserts: nodes = %d, want 2", got)
	}
	s.deleteNode("node-a")
	if got := len(s.snapshot(time.Now()).Nodes); got != 1 {
		t.Fatalf("after delete: nodes = %d, want 1", got)
	}
}

func TestClusterStateUpsertDeletePVAndPVC(t *testing.T) {
	s := newClusterState()
	s.upsertPV(planner.PVInfo{ObjectMeta: metav1.ObjectMeta{Name: "pv-1"}})
	s.upsertPVC(planner.PVCInfo{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pvc-1"}})
	snap := s.snapshot(time.Now())
	if len(snap.PVs) != 1 || len(snap.PVCs) != 1 {
		t.Fatalf("PVs=%d PVCs=%d, want 1/1", len(snap.PVs), len(snap.PVCs))
	}

	s.deletePV("pv-1")
	s.deletePVC(commontypes.NamespacedName{Namespace: "ns", Name: "pvc-1"})
	snap = s.snapshot(time.Now())
	if len(snap.PVs) != 0 || len(snap.PVCs) != 0 {
		t.Fatalf("after delete: PVs=%d PVCs=%d, want 0/0", len(snap.PVs), len(snap.PVCs))
	}
}

func TestClusterStateClassMutators(t *testing.T) {
	s := newClusterState()

	s.upsertStorageClass(storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "sc"}})
	s.upsertPriorityClass(schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "pc"}})
	s.upsertRuntimeClass(nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "rc"}})

	snap := s.snapshot(time.Now())
	if got := len(snap.StorageClasses); got != 1 {
		t.Fatalf("storageClasses = %d, want 1", got)
	}
	if got := len(snap.PriorityClasses); got != 1 {
		t.Fatalf("priorityClasses = %d, want 1", got)
	}
	if got := len(snap.RuntimeClasses); got != 1 {
		t.Fatalf("runtimeClasses = %d, want 1", got)
	}

	s.deleteStorageClass("sc")
	s.deletePriorityClass("pc")
	s.deleteRuntimeClass("rc")

	snap = s.snapshot(time.Now())
	if got := len(snap.StorageClasses); got != 0 {
		t.Fatalf("after delete storageClasses = %d, want 0", got)
	}
	if got := len(snap.PriorityClasses); got != 0 {
		t.Fatalf("after delete priorityClasses = %d, want 0", got)
	}
	if got := len(snap.RuntimeClasses); got != 0 {
		t.Fatalf("after delete runtimeClasses = %d, want 0", got)
	}
}

// TestSnapshotIsIndependent verifies that snapshot() returns slices not aliased to internal maps:
// later mutations must not change a snapshot already handed out.
func TestSnapshotIsIndependent(t *testing.T) {
	s := newClusterState()
	s.upsertPod(podInfo("ns", "p1"))

	first := s.snapshot(time.Now())
	s.upsertPod(podInfo("ns", "p2"))

	if got := len(first.Pods); got != 1 {
		t.Fatalf("first snapshot pods = %d, want 1 (snapshot aliased internal state)", got)
	}
	if got := len(s.snapshot(time.Now()).Pods); got != 2 {
		t.Fatalf("second snapshot pods = %d, want 2", got)
	}
}

// TestClusterStateConcurrentMutators is a smoke test for the RWMutex: many goroutines mutating and
// snapshotting in parallel must not race or panic. Run with -race to exercise the guarantee.
func TestClusterStateConcurrentMutators(t *testing.T) {
	s := newClusterState()

	const writers = 8
	const writesPerGoroutine = 200
	const reads = writers * writesPerGoroutine

	var wg sync.WaitGroup
	for id := range writers {
		wg.Go(func() {
			for j := range writesPerGoroutine {
				name := fmt.Sprintf("n-%d-%d", id, j)
				s.upsertNode(nodeInfo(name))
				s.deleteNode(name)
			}
		})
	}
	wg.Go(func() {
		for range reads {
			_ = s.snapshot(time.Now())
		}
	})
	wg.Wait()
}

// upcomingFor builds a synthetic upcoming entry with the given placement and deadline.
func upcomingFor(advUID types.UID, idx, replica int32, p corev1alpha1.NodePlacement, deadline time.Time) (upcomingKey, upcomingNode) {
	k := upcomingKey{advUID, idx, replica}
	return k, upcomingNode{
		key:              k,
		nodeInfo:         planner.NodeInfo{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("upc-%s-%d-%d", advUID, idx, replica)}},
		placement:        p,
		creationDeadline: deadline,
	}
}

func TestPruneExpiredAtSnapshot(t *testing.T) {
	s := newClusterState()
	p := corev1alpha1.NodePlacement{PoolName: "p", TemplateName: "t", InstanceType: "m5", Region: "r", AvailabilityZone: "z"}
	now := time.Now()
	k1, u1 := upcomingFor("u1", 0, 0, p, now.Add(-time.Minute))
	k2, u2 := upcomingFor("u1", 0, 1, p, now.Add(time.Minute))
	s.upcoming[k1], s.upcoming[k2] = u1, u2

	snap := s.snapshot(time.Now())
	if len(snap.UpcomingNodes) != 1 {
		t.Fatalf("UpcomingNodes=%d, want 1", len(snap.UpcomingNodes))
	}
}

func TestRemoveUpcomingMatchingConsumesOne(t *testing.T) {
	s := newClusterState()
	p := corev1alpha1.NodePlacement{PoolName: "p", TemplateName: "t", InstanceType: "m5", Region: "r", AvailabilityZone: "z"}
	deadline := time.Now().Add(time.Hour)
	for i := range int32(3) {
		k, u := upcomingFor("u1", 0, i, p, deadline)
		s.upcoming[k] = u
	}
	s.removeUpcomingMatching(p)
	if len(s.upcoming) != 2 {
		t.Fatalf("upcoming=%d, want 2", len(s.upcoming))
	}
}

func TestRemoveUpcomingMatchingNoMatch(t *testing.T) {
	s := newClusterState()
	p := corev1alpha1.NodePlacement{PoolName: "p", TemplateName: "t", InstanceType: "m5", Region: "r", AvailabilityZone: "z"}
	other := p
	other.AvailabilityZone = "z2"
	k, u := upcomingFor("u1", 0, 0, p, time.Now().Add(time.Hour))
	s.upcoming[k] = u
	s.removeUpcomingMatching(other)
	if len(s.upcoming) != 1 {
		t.Fatalf("upcoming=%d, want 1 (non-matching placement must not consume)", len(s.upcoming))
	}
}

func TestSnapshotUpcomingIndependent(t *testing.T) {
	s := newClusterState()
	p := corev1alpha1.NodePlacement{PoolName: "p", TemplateName: "t", InstanceType: "m5", Region: "r", AvailabilityZone: "z"}
	k, u := upcomingFor("u1", 0, 0, p, time.Now().Add(time.Hour))
	s.upcoming[k] = u

	first := s.snapshot(time.Now())
	k2, u2 := upcomingFor("u1", 0, 1, p, time.Now().Add(time.Hour))
	s.upcoming[k2] = u2

	if len(first.UpcomingNodes) != 1 {
		t.Fatalf("first snapshot UpcomingNodes=%d, want 1", len(first.UpcomingNodes))
	}
}

func TestMergeBackoffsForAdviceMaxWins(t *testing.T) {
	s := newClusterState()
	p := corev1alpha1.NodePlacement{PoolName: "p", TemplateName: "t", InstanceType: "m5", Region: "r", AvailabilityZone: "z"}
	t1 := time.Now().Add(time.Minute)
	t2 := t1.Add(time.Minute)

	s.mergeBackoffsForAdvice(map[corev1alpha1.NodePlacement]time.Time{p: t1})
	s.mergeBackoffsForAdvice(map[corev1alpha1.NodePlacement]time.Time{p: t2})
	if !s.backoffs[p].Equal(t2) {
		t.Fatalf("after max merge: got %v want %v", s.backoffs[p], t2)
	}

	s.mergeBackoffsForAdvice(map[corev1alpha1.NodePlacement]time.Time{p: t1})
	if !s.backoffs[p].Equal(t2) {
		t.Fatalf("smaller merge changed value: got %v want %v", s.backoffs[p], t2)
	}
}

func TestSetUpcomingForAdviceReplacesPriorEntries(t *testing.T) {
	s := newClusterState()
	p := corev1alpha1.NodePlacement{PoolName: "p", TemplateName: "t", InstanceType: "m5", Region: "r", AvailabilityZone: "z"}
	deadline := time.Now().Add(time.Hour)
	k1, u1 := upcomingFor("u1", 0, 0, p, deadline)
	k2, u2 := upcomingFor("u2", 0, 0, p, deadline)
	s.upcoming[k1], s.upcoming[k2] = u1, u2

	// Replace u1's entries with a single new one.
	newKey, newUp := upcomingFor("u1", 0, 0, p, deadline.Add(time.Minute))
	s.setUpcomingNodesForAdvice("u1", []upcomingNode{newUp})

	if len(s.upcoming) != 2 {
		t.Fatalf("upcoming=%d, want 2 (one for u1, one for u2)", len(s.upcoming))
	}
	if !s.upcoming[newKey].creationDeadline.Equal(deadline.Add(time.Minute)) {
		t.Fatal("u1 entry was not replaced")
	}
	if _, ok := s.upcoming[k2]; !ok {
		t.Fatal("u2 entry was wrongly purged")
	}
}

// TestRecordAdviceTransitions exercises the acked-now signal across the
// transitions recordAdvice cares about: first-seen, UID swap, and unack→ack.
func TestRecordAdviceTransitions(t *testing.T) {
	ref := commontypes.NamespacedName{Namespace: "ns", Name: "c1"}
	t0 := time.Now()

	s := newClusterState()

	// First-seen unacked advice → not ackedNow, but recorded.
	if ackedNow := s.recordAdvice(ref, adviceState{uid: "u1", creationTime: t0, acknowledged: false}); ackedNow {
		t.Fatal("first-seen unacked advice should not signal ackedNow")
	}
	if got := s.latestAdvice[ref]; got.uid != "u1" || got.acknowledged {
		t.Fatalf("latestAdvice=%+v, want uid=u1 acknowledged=false", got)
	}

	// Same UID transitions unacked → acked → ackedNow.
	if ackedNow := s.recordAdvice(ref, adviceState{uid: "u1", creationTime: t0, acknowledged: true}); !ackedNow {
		t.Fatal("unacked → acked transition should signal ackedNow")
	}

	// Re-recording the same acked state is not "now" — already acked.
	if ackedNow := s.recordAdvice(ref, adviceState{uid: "u1", creationTime: t0, acknowledged: true}); ackedNow {
		t.Fatal("re-recording an already-acked advice should not signal ackedNow")
	}

	// Newer advice with a different UID, already acknowledged → ackedNow (uid swap).
	if ackedNow := s.recordAdvice(ref, adviceState{uid: "u2", creationTime: t0.Add(time.Minute), acknowledged: true}); !ackedNow {
		t.Fatal("uid swap to acked advice should signal ackedNow")
	}
	if got := s.latestAdvice[ref]; got.uid != "u2" {
		t.Fatalf("latestAdvice.uid=%s, want u2", got.uid)
	}

	// Strictly older advice is rejected and does not overwrite.
	if ackedNow := s.recordAdvice(ref, adviceState{uid: "u-old", creationTime: t0.Add(-time.Hour), acknowledged: true}); ackedNow {
		t.Fatal("strictly older advice should not signal ackedNow")
	}
	if got := s.latestAdvice[ref]; got.uid != "u2" {
		t.Fatalf("older advice overwrote latest: latestAdvice.uid=%s, want u2", got.uid)
	}
}

// TestForgetAdviceMatchesUID confirms forgetAdvice only deletes when the
// stored UID matches — a stale forget for a previous UID is a no-op.
func TestForgetAdviceMatchesUID(t *testing.T) {
	ref := commontypes.NamespacedName{Namespace: "ns", Name: "c1"}
	s := newClusterState()
	s.recordAdvice(ref, adviceState{uid: "u1", creationTime: time.Now(), acknowledged: true})

	// Wrong UID → no-op.
	s.forgetAdvice(ref, "u-other")
	if _, ok := s.latestAdvice[ref]; !ok {
		t.Fatal("forgetAdvice with wrong UID should not delete entry")
	}

	// Matching UID → entry removed.
	s.forgetAdvice(ref, "u1")
	if _, ok := s.latestAdvice[ref]; ok {
		t.Fatal("forgetAdvice with matching UID should delete entry")
	}

	// Forgetting an unknown ref is a safe no-op.
	s.forgetAdvice(commontypes.NamespacedName{Namespace: "ns", Name: "missing"}, "u1")
}

// TestIsLatestAdviceAcknowledged covers the three states: unknown ref (true,
// nothing blocks), unacked (false), acked (true).
func TestIsLatestAdviceAcknowledged(t *testing.T) {
	ref := commontypes.NamespacedName{Namespace: "ns", Name: "c1"}
	s := newClusterState()

	if !s.isLatestAdviceAcknowledged(ref) {
		t.Fatal("unknown ref should report acknowledged=true (nothing to block on)")
	}

	s.recordAdvice(ref, adviceState{uid: "u1", creationTime: time.Now(), acknowledged: false})
	if s.isLatestAdviceAcknowledged(ref) {
		t.Fatal("unacked advice should report acknowledged=false")
	}

	s.recordAdvice(ref, adviceState{uid: "u1", creationTime: time.Now(), acknowledged: true})
	if !s.isLatestAdviceAcknowledged(ref) {
		t.Fatal("acked advice should report acknowledged=true")
	}
}
