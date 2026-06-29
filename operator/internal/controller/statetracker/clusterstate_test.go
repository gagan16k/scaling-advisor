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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// These tests exercise clusterState directly. Snapshot-level coverage (which
// now reads from the data-plane cache rather than in-memory maps) lives in
// statetracker_test.go.

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


func TestPruneExpiredAtUpcomingSnapshot(t *testing.T) {
	s := newClusterState()
	p := corev1alpha1.NodePlacement{PoolName: "p", TemplateName: "t", InstanceType: "m5", Region: "r", AvailabilityZone: "z"}
	now := time.Now()
	k1, u1 := upcomingFor("u1", 0, 0, p, now.Add(-time.Minute))
	k2, u2 := upcomingFor("u1", 0, 1, p, now.Add(time.Minute))
	s.upcoming[k1], s.upcoming[k2] = u1, u2

	got := s.upcomingSnapshot(now)
	if len(got) != 1 {
		t.Fatalf("upcomingSnapshot=%d, want 1", len(got))
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

func TestUpcomingSnapshotIndependent(t *testing.T) {
	s := newClusterState()
	p := corev1alpha1.NodePlacement{PoolName: "p", TemplateName: "t", InstanceType: "m5", Region: "r", AvailabilityZone: "z"}
	k, u := upcomingFor("u1", 0, 0, p, time.Now().Add(time.Hour))
	s.upcoming[k] = u

	first := s.upcomingSnapshot(time.Now())
	k2, u2 := upcomingFor("u1", 0, 1, p, time.Now().Add(time.Hour))
	s.upcoming[k2] = u2

	if len(first) != 1 {
		t.Fatalf("first snapshot=%d, want 1", len(first))
	}
	if got := len(s.upcomingSnapshot(time.Now())); got != 2 {
		t.Fatalf("second snapshot=%d, want 2", got)
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

// TestClusterStateConcurrentMutators is a smoke test for the RWMutex: many
// goroutines mutating advice/upcoming/seenNodes in parallel must not race or
// panic. Run with -race to exercise the guarantee.
func TestClusterStateConcurrentMutators(t *testing.T) {
	s := newClusterState()
	p := corev1alpha1.NodePlacement{PoolName: "p", TemplateName: "t", InstanceType: "m5", Region: "r", AvailabilityZone: "z"}

	const writers = 8
	const writesPerGoroutine = 200
	const reads = writers * writesPerGoroutine

	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			for range writesPerGoroutine {
				s.removeUpcomingMatching(p)
			}
		})
	}
	wg.Go(func() {
		for range reads {
			_ = s.upcomingSnapshot(time.Now())
		}
	})
	wg.Wait()
}
