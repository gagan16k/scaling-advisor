# Fake Actuator

A minimal stand-in for a lifecycle controller (`lcc`, e.g. MCM) for local testing
of the Scaling Advisor Operator.

It watches `ScalingAdvice` resources and writes an ack-shaped `ScalingFeedback`
into `ScalingAdvice.Status.Feedback`. That closes the operator's advice→feedback
contract so the operator's feedback-aware reconcile paths can be exercised
without a real `lcc`.

## What it does

For each `ScalingAdvice`:

- If `Status.Feedback` is already set, do nothing (idempotency).
- Otherwise build a `ScalingFeedback`:
  - For every `Spec.ScaleOutPlan.Items[i]`, emit a `ScaleOutItemFeedback` with
    `Index = i` and `CreationDeadline = now + --creation-delay`.
  - For every `Spec.ScaleInPlan.Items[i]`, append `Items[i].NodeName` to
    `ScaleIn.AcceptedNodesNames`.
- Patch `Status.Feedback`.

It does **not** create fake `Node` objects, reject items, set backoff, or refresh
deadlines. Those are explicit non-goals of this fixture.

## Run

```sh
go run ./cmd/fakeactuator \
    --kubeconfig=../../operator/testdata/kubeconfig.yaml \
    --creation-delay=30s
```

`--kubeconfig` should point at the same data plane the operator publishes
`ScalingAdvice` into (see `operator/testdata/local-operator-config.yaml`).
