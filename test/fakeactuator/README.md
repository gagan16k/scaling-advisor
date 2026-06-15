# Fake Actuator

A configurable stand-in for a lifecycle controller (`lcc`, e.g. MCM) used to exercise
the Scaling Advisor Operator end-to-end during local development.

It watches `ScalingAdvice` on a **control-plane** cluster, creates or deletes real
`Node` objects on a **data-plane** cluster according to per-placement failure rules,
and writes `ScalingFeedback` back into `ScalingAdvice.Status`.

## What it does

For each unacked `ScalingAdvice`:

1. **Scale-in**: delete named Nodes on the data-plane (`IgnoreNotFound`).
2. **Scale-out**: for each `ScaleOutItem`, consult failure rules and create 0–Delta
   Nodes on the data-plane with correct placement labels and `NodeReady=True`.
3. **Write feedback**: patch `ScalingAdvice.Status.Feedback` with per-item acks
   (including `ErrorType`, `BackoffUntil`, `FailCount` where applicable).

`Status.Feedback != nil` is the idempotency gate — already-acked advice is skipped.

## Config

All behaviour is driven by a YAML file passed via `--config`. Unknown keys are rejected.

```yaml
controlPlaneKubeconfig: ""   # falls back to in-cluster / $KUBECONFIG
dataPlaneKubeconfig:    ""   # falls back to controlPlaneKubeconfig when empty

timing:
  creationDelay:    30s    # CreationDeadline = now + this
  nodeCreateDelay:  0s     # delay before Node Create calls
  nodeReadyDelay:   0s     # delay before NodeReady=True flip (0 = synchronous)
  backoffDuration:  2m     # default BackoffUntil = now + this (rules may override)
  deleteGraceDelay: 0s     # delay before scale-in Delete

failureRules:              # first-match wins; unmatched items succeed
  - match:
      poolName:     pool-a      # empty string = wildcard
      templateName: ""
      instanceType: ""
      zone:         eu-west-1a
    mode: resourceExhausted     # see failure modes below
    backoffDuration: 5m         # optional override of timing.backoffDuration
    failCount: 0                # only for mode=partial
```

### Failure modes

| Mode | Nodes created | `ErrorType` | `BackoffUntil` |
|---|---|---|---|
| `resourceExhausted` | 0 | `ResourceExhaustedError` | `now + backoffDuration` |
| `creationTimeout` | 0 | `CreationTimeoutError` | `now + backoffDuration` |
| `partial` | `Delta - failCount` | — | set when `failCount > 0 && backoffDuration > 0` |
| `silentDeadlineMiss` | 0 | — | — |

`silentDeadlineMiss` writes a normal `CreationDeadline` but never creates Nodes.
The operator's `clusterstate` prunes the upcoming entry lazily after the deadline expires.

Items with no matching rule succeed (all Delta Nodes created, no error fields).

Items whose pool/template is not found in the constraint are treated as
`resourceExhausted` regardless of rules.

## Two kubeconfigs

`--control-plane-kubeconfig` and `--data-plane-kubeconfig` override the values in the
config file. When `dataPlaneKubeconfig` is empty, the actuator falls back to the
control-plane config (single-cluster mode).

## Run

```sh
# success — all placements succeed, Nodes appear immediately
make start CONFIG=testdata/config-success.yaml

# backoff — pool-a / eu-west-1a gets ResourceExhaustedError + 5 m backoff
make start CONFIG=testdata/config-backoff.yaml

# custom kubeconfigs
make start CONFIG=testdata/config-success.yaml \
    CONTROL_PLANE_KUBECONFIG=/path/to/cp.yaml \
    DATA_PLANE_KUBECONFIG=/path/to/dp.yaml
```

## Node name format

Created Nodes follow the same hostname pattern as the operator's upcoming-node synthesizer:

```
upcoming-<adviceUID>-<poolName>-<templateName>-<itemIndex>-<replicaIndex>
```

This guarantees the operator's `removeUpcomingMatching` call consumes the synthetic
upcoming slot on first-seen arrival.

## Scope

Not implemented (explicit non-goals):

- Probabilistic / random failure injection.
- Post-ack deadline refresh or backoff extension.
- `CSINode` creation.
- Pod-side simulation.
