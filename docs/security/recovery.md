# Recovery and fencing contract

Steward Forge persists recovery metadata in the same row-locked Lakebase workflow record as
the brief. A claim binds the worker ID, process owner, heartbeat, lease expiry, and monotonic
epoch. State writes, checkpoints, broker requests, receipts, and workflow transitions reject an
expired owner or an older epoch.

## Kill and restore ordering

A kill request increments the epoch, clears the active lease, and persists a checkpoint before
calling any external access-control layer. The checkpoint carries an operator deadline five
seconds after the request. This deadline covers checkpoint persistence, not completion of
external revocation.

The required external layers are Gateway access, Unity Catalog grants, and credentials. Each
adapter must apply the desired state and read it back. Lost acknowledgements are reconciled from
the observed state. Restore cannot begin until every kill layer is verified, and a prior kill
cannot replay once restore intent exists.

Checkpoints, transitions, kill operations, restores, and expired-lease recovery use caller-supplied
idempotency identifiers. Replays return the first result only when the complete request binding
matches. A resumed checkpoint can be consumed once.

## Current verification boundary

The controller, Lakebase persistence path, broker fence, concurrency behavior, restart behavior,
and in-memory adapter contract are covered by automated tests. The current synchronous Scrum
Master tracer does not invoke the recovery controller. Live Gateway, Unity Catalog, and credential
adapters are not implemented or deployed yet, so three-layer revocation is not claimed as an
observed workspace capability.
