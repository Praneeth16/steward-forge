# Steward Forge engineering instructions

## Product boundary

Steward Forge is a governed digital-worker reference implementation. Keep the public repository vendor-neutral and free of customer names, internal meeting details, personal workspace hosts, CLI profiles, emails, credentials, and identifiers copied from other projects.

## Architecture rules

- Models may propose actions. Deterministic code owns authorization, state transitions, approvals, test gates, deployment, and receipts.
- Workers do not call one another. They exchange versioned contracts through the orchestrator and ledger.
- Worker connections are read-only. Mutations pass through the capability broker.
- Every mutation requires an idempotency key and must return the original receipt on replay.
- Persist release intent before external side effects.
- Fence worker and orchestrator writes with monotonic epochs and compare-and-set transitions.
- Emit canonical evidence at trusted boundaries, not from worker-authored payloads.
- Never weaken a safety gate to make a demo pass.

## Portability and security

- Deploy with one Databricks Asset Bundle and overridable variables.
- Create dedicated resources; never adopt an existing Lakebase project or catalog by default.
- Keep secrets and local profiles out of Git.
- Use synthetic data only.
- Public documentation must distinguish implemented, deployed, verified, planned, and unavailable capabilities.

## Testing

- Write contract, argument-validation, idempotency, approval, fencing, and evidence-integrity tests before their implementations.
- Add integration tests for each vertical slice across API, ledger, worker, gate, and receipt layers.
- Test failure and replay paths, including stale workers, duplicate decisions, torn writes, timeouts, throttling, and lost acknowledgements.
- Run relevant tests continuously and the full suite before each release commit.

## Git

- Work on meaningful feature branches.
- Stage only files belonging to the completed unit.
- Use conventional commit messages.
- Do not commit private source material or generated credentials.
