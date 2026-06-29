// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statetracker

import (
	"context"
	"errors"
	"testing"
	"time"

	commonconstants "github.com/gardener/scaling-advisor/api/common/constants"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"
)

func newTestTracker() *ClusterStateTracker {
	b := &ClusterStateTracker{
		log:   logr.Discard(),
		state: newClusterState(),
	}
	b.synced.Store(true)
	return b
}

func nodeLabels(pool, template, region, zone string) map[string]string {
	return map[string]string{
		commonconstants.LabelNodePoolName:     pool,
		commonconstants.LabelNodeTemplateName: template,
		corev1.LabelTopologyRegion:            region,
		corev1.LabelTopologyZone:              zone,
		corev1.LabelInstanceTypeStable:        "m5.large",
		corev1.LabelArchStable:                "amd64",
		corev1.LabelHostname:                  "host-a",
	}
}

func TestSnapshotNotSyncedReturnsError(t *testing.T) {
	b := &ClusterStateTracker{log: logr.Discard(), state: newClusterState()}
	if _, err := b.Snapshot(context.Background(), time.Now()); !errors.Is(err, ErrNotSynced) {
		t.Errorf("Snapshot() error = %v, want ErrNotSynced", err)
	}
}

func TestNodeAddConsumesUpcoming(t *testing.T) {
	b := newTestTracker()

	p := corev1alpha1.NodePlacement{
		PoolName: "pool-a", TemplateName: "tmpl-a", InstanceType: "m5.large",
		Region: "r1", AvailabilityZone: "zone-a",
	}
	k, u := upcomingFor("u1", 0, 0, p, time.Now().Add(time.Hour))
	b.state.upcoming[k] = u

	h := b.nodeHandler().(toolscache.ResourceEventHandlerFuncs)
	h.AddFunc(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "node-a",
		Labels: nodeLabels("pool-a", "tmpl-a", "r1", "zone-a"),
	}})

	if got := len(b.state.upcoming); got != 0 {
		t.Fatalf("upcoming = %d, want 0", got)
	}
}

func TestNodeAddNoMatchingUpcoming(t *testing.T) {
	b := newTestTracker()

	p := corev1alpha1.NodePlacement{
		PoolName: "other-pool", TemplateName: "other", InstanceType: "m5.large",
		Region: "r1", AvailabilityZone: "zone-a",
	}
	k, u := upcomingFor("u1", 0, 0, p, time.Now().Add(time.Hour))
	b.state.upcoming[k] = u

	h := b.nodeHandler().(toolscache.ResourceEventHandlerFuncs)
	h.AddFunc(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "node-a",
		Labels: nodeLabels("pool-a", "tmpl-a", "r1", "zone-a"),
	}})

	if got := len(b.state.upcoming); got != 1 {
		t.Fatalf("upcoming = %d, want 1 (non-matching node must not consume)", got)
	}
}

func TestNodeAddMissingLabelsSkipped(t *testing.T) {
	b := newTestTracker()
	p := corev1alpha1.NodePlacement{
		PoolName: "p", TemplateName: "t", InstanceType: "m5.large", Region: "r", AvailabilityZone: "z",
	}
	k, u := upcomingFor("u1", 0, 0, p, time.Now().Add(time.Hour))
	b.state.upcoming[k] = u

	h := b.nodeHandler().(toolscache.ResourceEventHandlerFuncs)
	h.AddFunc(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})

	if got := len(b.state.upcoming); got != 1 {
		t.Fatalf("upcoming = %d, want 1 (unlabeled node must not consume)", got)
	}
}
