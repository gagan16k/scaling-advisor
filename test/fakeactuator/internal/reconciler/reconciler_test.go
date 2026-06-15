// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package reconciler_test

import (
	"context"
	"testing"
	"time"

	apicommon "github.com/gardener/scaling-advisor/api/common/types"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/test/fakeactuator/internal/config"
	"github.com/gardener/scaling-advisor/test/fakeactuator/internal/reconciler"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := k8sscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

// buildCPClient creates a fake control-plane client with status subresource for ScalingAdvice.
func buildCPClient(t *testing.T, scheme *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.ScalingAdvice{}).
		WithObjects(objs...).
		Build()
}

// buildDPClient creates a fake data-plane client with status subresource for Node.
func buildDPClient(t *testing.T, scheme *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Node{}).
		WithObjects(objs...).
		Build()
}

// reconcile calls reconciler.Reconcile for the given namespace/name.
func reconcile(t *testing.T, r *reconciler.Reconciler, ns, name string) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	})
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
}

// testConstraint builds a ScalingConstraint with one pool, one template, one zone.
func testConstraint(ns, name, pool, template, instanceType, region, zone string) *corev1alpha1.ScalingConstraint {
	return &corev1alpha1.ScalingConstraint{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1alpha1.ScalingConstraintSpec{
			NodePools: []corev1alpha1.NodePool{
				{
					Name:              pool,
					Region:            region,
					AvailabilityZones: []string{zone},
					NodeTemplates: []corev1alpha1.NodeTemplate{
						{
							Name:         template,
							InstanceType: instanceType,
							Architecture: "amd64",
							Capacity: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("4"),
								corev1.ResourceMemory: resource.MustParse("16Gi"),
							},
						},
					},
				},
			},
		},
	}
}

// testAdvice builds a ScalingAdvice with one scale-out item.
func testAdvice(ns, name, constraintName, pool, template, instanceType, region, zone string, delta int32) *corev1alpha1.ScalingAdvice {
	return &corev1alpha1.ScalingAdvice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			UID:       types.UID("test-uid-" + name),
		},
		Spec: corev1alpha1.ScalingAdviceSpec{
			ConstraintRef: apicommon.NamespacedName{Namespace: ns, Name: constraintName},
			ScaleOutPlan: &corev1alpha1.ScaleOutPlan{
				Items: []corev1alpha1.ScaleOutItem{
					{
						NodePlacement: corev1alpha1.NodePlacement{
							PoolName:         pool,
							TemplateName:     template,
							InstanceType:     instanceType,
							Region:           region,
							AvailabilityZone: zone,
						},
						Delta: delta,
					},
				},
			},
		},
	}
}

// getFeedback retrieves the feedback from the ScalingAdvice status.
func getFeedback(t *testing.T, cl client.Client, ns, name string) *corev1alpha1.ScalingFeedback {
	t.Helper()
	adv := &corev1alpha1.ScalingAdvice{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, adv); err != nil {
		t.Fatalf("get advice %s/%s: %v", ns, name, err)
	}
	return adv.Status.Feedback
}

// countNodes counts nodes in the dp fake client.
func countNodes(t *testing.T, cl client.Client) int {
	t.Helper()
	list := &corev1.NodeList{}
	if err := cl.List(context.Background(), list); err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	return len(list.Items)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const (
	testNS           = "default"
	testPool         = "pool-a"
	testTemplate     = "tmpl-a"
	testInstanceType = "m5.xlarge"
	testRegion       = "eu-west-1"
	testZone         = "eu-west-1a"
)

// Test 1: Scale-out happy path — no rules, Delta=2, expect 2 Nodes with correct labels and ready.
func TestScaleOutHappyPath(t *testing.T) {
	scheme := testScheme(t)
	constraint := testConstraint(testNS, "sc", testPool, testTemplate, testInstanceType, testRegion, testZone)
	advice := testAdvice(testNS, "adv", "sc", testPool, testTemplate, testInstanceType, testRegion, testZone, 2)
	cpClient := buildCPClient(t, scheme, constraint, advice)
	dpClient := buildDPClient(t, scheme)

	r := reconciler.NewReconcilerForTest(cpClient, dpClient, nil)
	reconcile(t, r, testNS, "adv")

	fb := getFeedback(t, cpClient, testNS, "adv")
	if fb == nil {
		t.Fatal("expected feedback, got nil")
	}
	if fb.ScaleOut == nil || len(fb.ScaleOut.Items) != 1 {
		t.Fatalf("expected 1 scale-out item, got %v", fb.ScaleOut)
	}
	item := fb.ScaleOut.Items[0]
	if item.ErrorType != "" {
		t.Errorf("unexpected ErrorType: %v", item.ErrorType)
	}
	if item.BackoffUntil != nil {
		t.Errorf("unexpected BackoffUntil: %v", item.BackoffUntil)
	}
	if item.CreationDeadline.IsZero() {
		t.Error("CreationDeadline is zero")
	}

	if got := countNodes(t, dpClient); got != 2 {
		t.Errorf("expected 2 nodes, got %d", got)
	}

	// Verify labels on one of the nodes.
	list := &corev1.NodeList{}
	if err := dpClient.List(context.Background(), list); err != nil {
		t.Fatal(err)
	}
	node := list.Items[0]
	if node.Labels[corev1.LabelTopologyZone] != testZone {
		t.Errorf("zone label wrong: %v", node.Labels)
	}
	if node.Labels[corev1.LabelInstanceTypeStable] != testInstanceType {
		t.Errorf("instance-type label wrong: %v", node.Labels)
	}

	// Status capacity must be non-empty.
	if node.Status.Capacity.Cpu().IsZero() {
		t.Error("node CPU capacity is zero")
	}
	// NodeReady condition must be True.
	var ready corev1.ConditionStatus
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			ready = c.Status
		}
	}
	if ready != corev1.ConditionTrue {
		t.Errorf("NodeReady=%v, want True", ready)
	}
}

// Test 2: Scale-in happy path — seed dp with target nodes, reconcile → nodes deleted.
func TestScaleInHappyPath(t *testing.T) {
	scheme := testScheme(t)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-to-delete"}}
	constraint := testConstraint(testNS, "sc", testPool, testTemplate, testInstanceType, testRegion, testZone)
	advice := &corev1alpha1.ScalingAdvice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS,
			Name:      "adv",
			UID:       "test-uid-adv",
		},
		Spec: corev1alpha1.ScalingAdviceSpec{
			ConstraintRef: apicommon.NamespacedName{Namespace: testNS, Name: "sc"},
			ScaleInPlan: &corev1alpha1.ScaleInPlan{
				Items: []corev1alpha1.ScaleInItem{
					{NodeName: "node-to-delete"},
				},
			},
		},
	}
	cpClient := buildCPClient(t, scheme, constraint, advice)
	dpClient := buildDPClient(t, scheme, node)

	r := reconciler.NewReconcilerForTest(cpClient, dpClient, nil)
	reconcile(t, r, testNS, "adv")

	if got := countNodes(t, dpClient); got != 0 {
		t.Errorf("expected 0 nodes after scale-in, got %d", got)
	}

	fb := getFeedback(t, cpClient, testNS, "adv")
	if fb == nil || fb.ScaleIn == nil {
		t.Fatal("expected scale-in feedback")
	}
	if len(fb.ScaleIn.AcceptedNodesNames) != 1 || fb.ScaleIn.AcceptedNodesNames[0] != "node-to-delete" {
		t.Errorf("AcceptedNodesNames wrong: %v", fb.ScaleIn.AcceptedNodesNames)
	}
}

// Test 3: Idempotency — Status.Feedback != nil → no dp changes.
func TestIdempotency(t *testing.T) {
	scheme := testScheme(t)
	advice := testAdvice(testNS, "adv", "sc", testPool, testTemplate, testInstanceType, testRegion, testZone, 2)
	advice.Status.Feedback = &corev1alpha1.ScalingFeedback{
		ConstraintRef: apicommon.NamespacedName{Namespace: testNS, Name: "sc"},
	}
	cpClient := buildCPClient(t, scheme, advice)
	dpClient := buildDPClient(t, scheme)

	r := reconciler.NewReconcilerForTest(cpClient, dpClient, nil)
	reconcile(t, r, testNS, "adv")

	if got := countNodes(t, dpClient); got != 0 {
		t.Errorf("expected 0 nodes (idempotency gate), got %d", got)
	}
}

// Test 4: Missing pool/template — bad placement → ResourceExhausted, FailCount=Delta, no Nodes.
func TestMissingPoolTemplate(t *testing.T) {
	scheme := testScheme(t)
	// Constraint exists but has no matching template.
	constraint := testConstraint(testNS, "sc", "other-pool", testTemplate, testInstanceType, testRegion, testZone)
	advice := testAdvice(testNS, "adv", "sc", testPool, testTemplate, testInstanceType, testRegion, testZone, 3)
	cpClient := buildCPClient(t, scheme, constraint, advice)
	dpClient := buildDPClient(t, scheme)

	r := reconciler.NewReconcilerForTest(cpClient, dpClient, nil)
	reconcile(t, r, testNS, "adv")

	fb := getFeedback(t, cpClient, testNS, "adv")
	if fb == nil || fb.ScaleOut == nil {
		t.Fatal("expected scale-out feedback")
	}
	item := fb.ScaleOut.Items[0]
	if item.ErrorType != corev1alpha1.ScalingErrorTypeResourceExhausted {
		t.Errorf("ErrorType=%v, want ResourceExhaustedError", item.ErrorType)
	}
	if item.FailCount != 3 {
		t.Errorf("FailCount=%d, want 3", item.FailCount)
	}
	if got := countNodes(t, dpClient); got != 0 {
		t.Errorf("expected 0 nodes, got %d", got)
	}
}

// Test 5: Rule resourceExhausted → 0 nodes, ErrorType, BackoffUntil, FailCount=Delta.
func TestRuleResourceExhausted(t *testing.T) {
	scheme := testScheme(t)
	constraint := testConstraint(testNS, "sc", testPool, testTemplate, testInstanceType, testRegion, testZone)
	advice := testAdvice(testNS, "adv", "sc", testPool, testTemplate, testInstanceType, testRegion, testZone, 2)
	cpClient := buildCPClient(t, scheme, constraint, advice)
	dpClient := buildDPClient(t, scheme)

	cfg := config.Default()
	cfg.FailureRules = []config.FailureRule{
		{
			Match:           config.MatchCriteria{PoolName: testPool},
			Mode:            config.ModeResourceExhausted,
			BackoffDuration: 5 * time.Minute,
		},
	}
	r := reconciler.NewReconcilerForTest(cpClient, dpClient, cfg)
	reconcile(t, r, testNS, "adv")

	fb := getFeedback(t, cpClient, testNS, "adv")
	if fb == nil || fb.ScaleOut == nil {
		t.Fatal("expected scale-out feedback")
	}
	item := fb.ScaleOut.Items[0]
	if item.ErrorType != corev1alpha1.ScalingErrorTypeResourceExhausted {
		t.Errorf("ErrorType=%v, want ResourceExhaustedError", item.ErrorType)
	}
	if item.FailCount != 2 {
		t.Errorf("FailCount=%d, want 2", item.FailCount)
	}
	if item.BackoffUntil == nil {
		t.Error("BackoffUntil should be set")
	}
	if got := countNodes(t, dpClient); got != 0 {
		t.Errorf("expected 0 nodes, got %d", got)
	}
}

// Test 6: Rule creationTimeout — same shape, different ErrorType.
func TestRuleCreationTimeout(t *testing.T) {
	scheme := testScheme(t)
	constraint := testConstraint(testNS, "sc", testPool, testTemplate, testInstanceType, testRegion, testZone)
	advice := testAdvice(testNS, "adv", "sc", testPool, testTemplate, testInstanceType, testRegion, testZone, 1)
	cpClient := buildCPClient(t, scheme, constraint, advice)
	dpClient := buildDPClient(t, scheme)

	cfg := config.Default()
	cfg.FailureRules = []config.FailureRule{
		{
			Match:           config.MatchCriteria{PoolName: testPool},
			Mode:            config.ModeCreationTimeout,
			BackoffDuration: 2 * time.Minute,
		},
	}
	r := reconciler.NewReconcilerForTest(cpClient, dpClient, cfg)
	reconcile(t, r, testNS, "adv")

	fb := getFeedback(t, cpClient, testNS, "adv")
	item := fb.ScaleOut.Items[0]
	if item.ErrorType != corev1alpha1.ScalingErrorTypeCreationTimeout {
		t.Errorf("ErrorType=%v, want CreationTimeoutError", item.ErrorType)
	}
	if item.BackoffUntil == nil {
		t.Error("BackoffUntil should be set")
	}
	if got := countNodes(t, dpClient); got != 0 {
		t.Errorf("expected 0 nodes, got %d", got)
	}
}

// Test 7: Rule partial(failCount=1) with Delta=3 → 2 Nodes, FailCount=1.
func TestRulePartial(t *testing.T) {
	scheme := testScheme(t)
	constraint := testConstraint(testNS, "sc", testPool, testTemplate, testInstanceType, testRegion, testZone)
	advice := testAdvice(testNS, "adv", "sc", testPool, testTemplate, testInstanceType, testRegion, testZone, 3)
	cpClient := buildCPClient(t, scheme, constraint, advice)
	dpClient := buildDPClient(t, scheme)

	cfg := config.Default()
	cfg.FailureRules = []config.FailureRule{
		{
			Match:     config.MatchCriteria{PoolName: testPool},
			Mode:      config.ModePartial,
			FailCount: 1,
		},
	}
	r := reconciler.NewReconcilerForTest(cpClient, dpClient, cfg)
	reconcile(t, r, testNS, "adv")

	fb := getFeedback(t, cpClient, testNS, "adv")
	item := fb.ScaleOut.Items[0]
	if item.FailCount != 1 {
		t.Errorf("FailCount=%d, want 1", item.FailCount)
	}
	if item.ErrorType != "" {
		t.Errorf("unexpected ErrorType: %v", item.ErrorType)
	}
	if got := countNodes(t, dpClient); got != 2 {
		t.Errorf("expected 2 nodes (Delta-failCount), got %d", got)
	}
}

// Test 8: Rule silentDeadlineMiss → 0 Nodes, ack has CreationDeadline, no error fields.
func TestRuleSilentDeadlineMiss(t *testing.T) {
	scheme := testScheme(t)
	constraint := testConstraint(testNS, "sc", testPool, testTemplate, testInstanceType, testRegion, testZone)
	advice := testAdvice(testNS, "adv", "sc", testPool, testTemplate, testInstanceType, testRegion, testZone, 2)
	cpClient := buildCPClient(t, scheme, constraint, advice)
	dpClient := buildDPClient(t, scheme)

	cfg := config.Default()
	cfg.FailureRules = []config.FailureRule{
		{
			Match:         config.MatchCriteria{PoolName: testPool},
			Mode:          config.ModeSilentDeadlineMiss,
			CreationDelay: 1 * time.Second,
		},
	}
	r := reconciler.NewReconcilerForTest(cpClient, dpClient, cfg)
	reconcile(t, r, testNS, "adv")

	fb := getFeedback(t, cpClient, testNS, "adv")
	if fb == nil || fb.ScaleOut == nil {
		t.Fatal("expected scale-out feedback")
	}
	item := fb.ScaleOut.Items[0]
	if item.ErrorType != "" {
		t.Errorf("ErrorType=%v, want empty", item.ErrorType)
	}
	if item.BackoffUntil != nil {
		t.Errorf("BackoffUntil should be nil, got %v", item.BackoffUntil)
	}
	if item.CreationDeadline.IsZero() {
		t.Error("CreationDeadline must be set")
	}
	if got := countNodes(t, dpClient); got != 0 {
		t.Errorf("expected 0 nodes (silent miss), got %d", got)
	}
}

// Test 9: Wildcard match — empty templateName/zone matches everything in the pool.
func TestRuleWildcardMatch(t *testing.T) {
	scheme := testScheme(t)
	constraint := testConstraint(testNS, "sc", testPool, testTemplate, testInstanceType, testRegion, testZone)
	advice := testAdvice(testNS, "adv", "sc", testPool, testTemplate, testInstanceType, testRegion, testZone, 2)
	cpClient := buildCPClient(t, scheme, constraint, advice)
	dpClient := buildDPClient(t, scheme)

	cfg := config.Default()
	cfg.FailureRules = []config.FailureRule{
		{
			// Only PoolName set; TemplateName/InstanceType/Zone are wildcards.
			Match: config.MatchCriteria{PoolName: testPool},
			Mode:  config.ModeResourceExhausted,
		},
	}
	r := reconciler.NewReconcilerForTest(cpClient, dpClient, cfg)
	reconcile(t, r, testNS, "adv")

	fb := getFeedback(t, cpClient, testNS, "adv")
	item := fb.ScaleOut.Items[0]
	if item.ErrorType != corev1alpha1.ScalingErrorTypeResourceExhausted {
		t.Errorf("wildcard rule should have matched: ErrorType=%v", item.ErrorType)
	}
	if got := countNodes(t, dpClient); got != 0 {
		t.Errorf("expected 0 nodes, got %d", got)
	}
}

// Test 10: Crash recovery — AlreadyExists on Create is handled gracefully.
func TestCrashRecoveryAlreadyExists(t *testing.T) {
	scheme := testScheme(t)
	constraint := testConstraint(testNS, "sc", testPool, testTemplate, testInstanceType, testRegion, testZone)
	advice := testAdvice(testNS, "adv", "sc", testPool, testTemplate, testInstanceType, testRegion, testZone, 1)
	cpClient := buildCPClient(t, scheme, constraint, advice)

	// Pre-seed the data-plane with the node that would have been created.
	// Name matches the upcoming-node format: upcoming-<uid>-<pool>-<template>-<itemIdx>-<replicaIdx>
	existingNodeName := "upcoming-test-uid-adv-pool-a-tmpl-a-0-0"
	existingNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: existingNodeName},
	}
	dpClient := buildDPClient(t, scheme, existingNode)

	r := reconciler.NewReconcilerForTest(cpClient, dpClient, nil)
	reconcile(t, r, testNS, "adv")

	// Feedback must still be written.
	fb := getFeedback(t, cpClient, testNS, "adv")
	if fb == nil {
		t.Fatal("feedback should be written even after AlreadyExists")
	}

	// Node count: still 1 (the pre-existing one, possibly with status updated).
	if got := countNodes(t, dpClient); got != 1 {
		t.Errorf("expected 1 node, got %d", got)
	}
}

// Test 11: Constraint not found → reconcile returns nil (no feedback written yet).
func TestConstraintNotFound(t *testing.T) {
	scheme := testScheme(t)
	advice := testAdvice(testNS, "adv", "missing-constraint", testPool, testTemplate, testInstanceType, testRegion, testZone, 1)
	cpClient := buildCPClient(t, scheme, advice)
	dpClient := buildDPClient(t, scheme)

	r := reconciler.NewReconcilerForTest(cpClient, dpClient, nil)
	reconcile(t, r, testNS, "adv")

	fb := getFeedback(t, cpClient, testNS, "adv")
	if fb != nil {
		t.Errorf("feedback should not be written when constraint is missing, got %v", fb)
	}
}

// Test 12: Advice not found → no error.
func TestAdviceNotFound(t *testing.T) {
	scheme := testScheme(t)
	cpClient := buildCPClient(t, scheme)
	dpClient := buildDPClient(t, scheme)

	r := reconciler.NewReconcilerForTest(cpClient, dpClient, nil)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: "nonexistent"},
	})
	if err != nil {
		t.Errorf("expected nil error for missing advice, got %v", err)
	}
}

// Test 13: Scale-in with already-deleted node → IgnoreNotFound, feedback still written.
func TestScaleInIgnoresNotFound(t *testing.T) {
	scheme := testScheme(t)
	constraint := testConstraint(testNS, "sc", testPool, testTemplate, testInstanceType, testRegion, testZone)
	advice := &corev1alpha1.ScalingAdvice{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "adv", UID: "uid-adv"},
		Spec: corev1alpha1.ScalingAdviceSpec{
			ConstraintRef: apicommon.NamespacedName{Namespace: testNS, Name: "sc"},
			ScaleInPlan: &corev1alpha1.ScaleInPlan{
				Items: []corev1alpha1.ScaleInItem{
					{NodeName: "already-gone"},
				},
			},
		},
	}
	cpClient := buildCPClient(t, scheme, constraint, advice)
	dpClient := buildDPClient(t, scheme) // node not pre-seeded

	r := reconciler.NewReconcilerForTest(cpClient, dpClient, nil)
	reconcile(t, r, testNS, "adv")

	fb := getFeedback(t, cpClient, testNS, "adv")
	if fb == nil {
		t.Fatal("expected feedback to be written")
	}
}

// Verify the scale-in node actually disappears from dp.
func TestScaleInNodeGone(t *testing.T) {
	scheme := testScheme(t)
	constraint := testConstraint(testNS, "sc", testPool, testTemplate, testInstanceType, testRegion, testZone)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "delete-me"}}
	advice := &corev1alpha1.ScalingAdvice{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "adv", UID: "uid-adv"},
		Spec: corev1alpha1.ScalingAdviceSpec{
			ConstraintRef: apicommon.NamespacedName{Namespace: testNS, Name: "sc"},
			ScaleInPlan: &corev1alpha1.ScaleInPlan{
				Items: []corev1alpha1.ScaleInItem{{NodeName: "delete-me"}},
			},
		},
	}
	cpClient := buildCPClient(t, scheme, constraint, advice)
	dpClient := buildDPClient(t, scheme, node)

	r := reconciler.NewReconcilerForTest(cpClient, dpClient, nil)
	reconcile(t, r, testNS, "adv")

	got := &corev1.Node{}
	err := dpClient.Get(context.Background(), types.NamespacedName{Name: "delete-me"}, got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected node to be deleted, get returned err=%v node=%v", err, got)
	}
}
