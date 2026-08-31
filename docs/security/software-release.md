# Software Engineer release boundary

The version 1 Software Engineer can read synthetic table names only inside the task's
configured sandbox catalog and schema, then draft a dashboard candidate. It has no
repository or deployment adapter. Its only mutation request
asks the capability broker to create a candidate commit on one configured branch and under
one configured `generated/**` prefix. The broker rejects traversal, platform,
infrastructure, secret, resource, and `.github/**` paths. It also rejects harmful or
secret-like content before the repository adapter runs. No push tool is registered.

The primary candidate is a self-contained dashboard with backlog, pipeline-reliability, and
platform-cost signals. A Genie specification is present only when the task records that
creation was verified and supplies an evidence ID. An unverified Genie request cannot replace
or delay the dashboard.

Six deterministic checks run independently: unit, integration, quality, policy, secret, and
harmful diff. Candidate files are treated as data and are never executed in the trusted gate
process. The release service requires all six named checks, a validated named approver who is
not the submitter, an approval for the exact candidate commit SHA, and ancestry from the
configured trusted base. It binds each validated decision ID to its full content before
interpreting approval or rejection, preventing a rejected or failed decision from being
mutated on retry. A durable release intent commits with the exact approval before
deployment. The deployment boundary observes remote state before mutation, returns stable
workspace IDs and the prior rollback state, and binds the result to the intent request hash
and receipt ID. A lost acknowledgement can resume without another deployment or another
charged worker attempt.

The governed receipt links the brief, generated code, synthetic-data receipt, approvals,
all six gates, cost, explicit model-use status, deployment output, and a protected
evidence-chain reference. Insert-only receipt and pointer stores reject a same-ID content
conflict and reconcile either missing side after a partial write.

The in-memory repository, deployment backend, receipt store, and pointer store prove the
local control flow and restart behavior without changing GitHub or a Databricks workspace.
Live candidate-branch creation, dashboard deployment, Genie creation, Delta receipt
publication, Lakebase pointer publication, returned workspace identifiers, and rollback
execution remain unverified until dedicated adapters run in a target environment. The local
IDs are test fixtures and are not presented as platform evidence.
