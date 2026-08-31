# Data Engineer pipeline boundary

The version 1 Data Engineer task fixes the seed, brief ID, run ID, sandbox catalog,
sandbox schema, and a maximum of one repair attempt. The worker derives all table names
from those values. It cannot submit an arbitrary table name.

Every publish request crosses the capability broker as structured, non-executable data.
The broker compares the requested catalog and schema with the registered worker contract,
denies an outside target, and records the denial. Instruction canaries stay inside row
fields. The worker never evaluates those fields as commands, and the repair function can
change only the three documented quality-defect records.

The worker returns two small generated artifacts, pipeline code and its tests. The candidate
manifest records their hashes, the published table hashes, a lineage hash, and the repair
count. It does not copy dataset rows into the manifest. The final receipt binds that manifest
to the broker mutation receipts and deterministic gate results.

Local tests use `InMemoryCatalogAdapter`. That adapter proves the task, contract, repair,
catalog-output, gate, progress, replay, denial-log, and receipt chain without claiming a live
Unity Catalog write. A live Unity Catalog adapter and a dedicated worker identity with
sandbox-only platform grants remain deployment work. Until both are exercised in a target
workspace, only the broker-level sandbox denial is verified.
