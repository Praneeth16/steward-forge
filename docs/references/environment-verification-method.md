# Environment verification method

Each platform claim must have five fields:

1. Capability being tested.
2. Exact command or user action.
3. Dated observed result from the target workspace.
4. Pass, fail, or unavailable status.
5. Product documentation source and the fallback when the result is not a pass.

Documentation proves that a feature can exist. Only an observed target-workspace result proves that this deployment can use it. Never replace an unavailable result with a documentation claim.

Do not include tokens, personal emails, workspace hosts, CLI profile names, account IDs, or unrelated resource names in the public evidence. Store sensitive raw output outside Git and publish a redacted summary.
