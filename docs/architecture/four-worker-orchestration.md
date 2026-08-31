# Four-worker orchestration

## Implemented local reference flow

`DeliveryCoordinator` runs one approved brief through Product Manager, Scrum
Master, Data Engineer, and Software Engineer contracts. The original tracer
orchestrator remains unchanged. The reference flow uses the existing governed
Data Engineer pipeline and SHA-bound Software Engineer release service.

The Product Manager emits a versioned outcome, scope, assumptions, and typed
acceptance tests. Deterministic policy code checks the named approver and rejects
an approval from the Product Manager identity. The Scrum Master then creates two
bounded tasks with dependency order, attempt limits, cost allocations, stop
conditions, and expected outputs. Its escalation contract names the orchestrator
as the only retry owner.

After planning, the coordinator prepares the Data Engineer and Software Engineer
candidates concurrently. These preparation methods only generate in-memory data
and artifacts. Catalog writes, candidate commits, and deployment adapter calls
enter one coordinator-owned mutation lane, so they cannot overlap. Per-workflow
phase locks prevent duplicate calls inside one App process. Durable cross-replica
phase ownership and broker receipt recovery are not claimed here.

Each specialist task claims a fenced recovery lease. Before a retry, the
coordinator persists the failed attempt and a recovery checkpoint. It consumes a
fixed task budget for every attempt and terminates with `succeeded`,
`budget_stopped`, or `failed`. The Scrum Master may report the failure, but it
cannot invoke a retry or another worker.

Every coordinator process has a distinct identity in its lease owner. An active
lease held by another coordinator means work is still in progress; it does not
fail the workflow or authorize a repeated mutation. The durable row fence renews
the same owner and epoch after a successful mutation, and a completed transition
releases the lease at the same atomic boundary. Task terminal state, run terminal
state, and their evidence commit in that boundary as well.

The Scrum plan binds the brief ID, scope version, and canonical fingerprint of
the exact approved scope. PM and SM outputs are serialized and revalidated at the
coordinator boundary before persistence. Candidate preparation has its own
bounded attempt counter but shares the Software Engineer task budget with release
attempts. Each workflow derives a deterministic candidate branch beneath the
configured branch prefix, so independent exact-SHA candidates never advance the
same branch head.

## Contract and evidence boundaries

Workers do not import or invoke peer workers. They accept versioned input
contracts and return proposals or candidates to deterministic services. The
coordinator owns approval checks, task order, attempts, budget decisions,
recovery transitions, mutation serialization, and final run state.

The reference result carries ordered evidence for submission, scope proposal and
approval, plan creation, read-only preparation, every attempt, escalation,
checkpoint, each governed receipt, and the terminal run state. Data evidence
includes the existing Data Engineer receipt. Software evidence includes the
existing broker receipt, isolated gate results, approval-bound commit SHA,
deployment result, and rollback state.

The first exact release decision atomically moves the workflow from
`release_pending` to `release_in_progress`. Only an identical replay may resume
that decision; a conflicting approval or rejection is refused. Final completion
compares the stored decision binding before it can commit.

## Verification boundary

The integration test executes the complete flow with deterministic synthetic
data and local in-memory adapters. It proves contract behavior, gates, receipts,
retry ownership, budget stops, failure states, concurrent reads, and serialized
mutations.

This local reference does not prove a live Unity Catalog write, GitHub mutation,
Databricks dashboard or Genie deployment, workspace resource ID, or rollback.
Broker receipt caches are process-local, so a crash between an external write and
its committed transition still requires the outbox and reconciliation work in the
next evidence slice. The local catalog adapter makes repeated identical writes
idempotent, but that is not cross-system crash proof. Those claims require
deployment evidence from the target workspace and remote systems. Later delivery
slices add durable release evidence, model limits, adversarial proof gates, and
complete live deployment evidence.
