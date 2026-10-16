"""e2e step: fail the run when an earlier step failed."""

raise RuntimeError("An e2e step failed; see the failed task run.")
