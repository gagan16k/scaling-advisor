// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package clusterstate

import (
	"fmt"
	"sync"
	"testing"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	"github.com/gardener/scaling-advisor/api/planner"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	if got := len(s.snapshot().Pods); got != 1 {
		t.Fatalf("after upsert: pods = %d, want 1", got)
	}

	// Upsert with same key replaces, doesn't grow the map.
	s.upsertPod(podInfo("ns", "p"))
	if got := len(s.snapshot().Pods); got != 1 {
		t.Fatalf("after second upsert: pods = %d, want 1", got)
	}

	s.deletePod(key)
	if got := len(s.snapshot().Pods); got != 0 {
		t.Fatalf("after delete: pods = %d, want 0", got)
	}
}

func TestClusterStateUpsertDeleteNode(t *testing.T) {
	s := newClusterState()
	s.upsertNode(nodeInfo("node-a"))
	s.upsertNode(nodeInfo("node-b"))
	if got := len(s.snapshot().Nodes); got != 2 {
		t.Fatalf("after two upserts: nodes = %d, want 2", got)
	}
	s.deleteNode("node-a")
	if got := len(s.snapshot().Nodes); got != 1 {
		t.Fatalf("after delete: nodes = %d, want 1", got)
	}
}

func TestClusterStateUpsertDeletePVAndPVC(t *testing.T) {
	s := newClusterState()
	s.upsertPV(planner.PVInfo{ObjectMeta: metav1.ObjectMeta{Name: "pv-1"}})
	s.upsertPVC(planner.PVCInfo{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pvc-1"}})
	snap := s.snapshot()
	if len(snap.PVs) != 1 || len(snap.PVCs) != 1 {
		t.Fatalf("PVs=%d PVCs=%d, want 1/1", len(snap.PVs), len(snap.PVCs))
	}

	s.deletePV("pv-1")
	s.deletePVC(commontypes.NamespacedName{Namespace: "ns", Name: "pvc-1"})
	snap = s.snapshot()
	if len(snap.PVs) != 0 || len(snap.PVCs) != 0 {
		t.Fatalf("after delete: PVs=%d PVCs=%d, want 0/0", len(snap.PVs), len(snap.PVCs))
	}
}

func TestClusterStateClassMutators(t *testing.T) {
	s := newClusterState()
	s.upsertStorageClass(storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "sc"}})
	if got := len(s.snapshot().StorageClasses); got != 1 {
		t.Fatalf("storageClasses = %d, want 1", got)
	}
	s.deleteStorageClass("sc")
	if got := len(s.snapshot().StorageClasses); got != 0 {
		t.Fatalf("after delete storageClasses = %d, want 0", got)
	}
}

// TestSnapshotIsIndependent verifies that snapshot() returns slices not aliased to internal maps:
// later mutations must not change a snapshot already handed out.
func TestSnapshotIsIndependent(t *testing.T) {
	s := newClusterState()
	s.upsertPod(podInfo("ns", "p1"))

	first := s.snapshot()
	s.upsertPod(podInfo("ns", "p2"))

	if got := len(first.Pods); got != 1 {
		t.Fatalf("first snapshot pods = %d, want 1 (snapshot aliased internal state)", got)
	}
	if got := len(s.snapshot().Pods); got != 2 {
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
			_ = s.snapshot()
		}
	})
	wg.Wait()
}
