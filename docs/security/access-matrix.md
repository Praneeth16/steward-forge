# Access and identity contract

Steward Forge derives the caller from the Databricks Apps forwarded access token. Request
bodies cannot supply the submitter or decision actor. The orchestrator validates the token
against the workspace current-user API and maps workspace groups to roles through DAB
variables.

| Role | Endpoints | Row visibility | Explicit denials |
| --- | --- | --- | --- |
| Submitter | Create brief, read own brief | Briefs whose token subject equals `submitted_by` | Approval unless separately assigned the approver role; release of their own brief is always denied; operator controls |
| Viewer | Read brief, model budget, and payload-free trace metadata | Briefs where the token subject is submitter, named approver, or listed viewer | Create, approve, operate, audit export |
| Approver | Read brief, decide scope, decide release | Briefs where the token subject is the named approver | Release when also the submitter; decisions for another approver or version/SHA |
| Operator | Read operational brief state, kill, restore, recover | All operational rows; evidence mutations remain denied | Submitter and approver actions unless separately assigned |
| Auditor | Read briefs, evidence, receipts, and traces | All governed evidence rows | Workflow, approval, recovery, and release mutations |

The development defaults map submitter, viewer, and approver to the built-in `users` group.
The exact named-approver check and submitter-versus-approver separation still apply. Operator
and auditor default to `account admins`. Deployers override each group with bundle variables
without source changes.

## Decision binding

- Scope decisions bind to `decision_id`, validated subject, and exact `scope_version`.
- Release decisions bind to `decision_id`, validated subject, and exact candidate SHA.
- The approval and transition occur within one ledger transaction.
- Replaying an identical decision returns the first state without another event.
- Reusing a decision ID with different content fails and produces a denial event.
- Missing, invalid, stale, unauthorized, or out-of-order decisions fail closed.

Cross-App token forwarding requires a separate live probe once the workbench and orchestrator
run as distinct Apps. The current single-App tracer proves token validation at the API boundary
but does not claim that cross-App OBO is working.
