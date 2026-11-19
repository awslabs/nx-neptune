Changelog
=========

Unreleased
----------

### Behaviour changes

`SessionManager` cleanup on context exit changed. These affect existing users:

- `CleanupTask.STOP` now runs when the `with` block exits. Previously only
  `DESTROY` did, so a session configured with `STOP` left its graphs running
  and billed.
- `CleanupTask.RESET` now runs on exit too, and **deletes all data** from every
  matching graph. Previously it did nothing on exit, so a session configured
  with `RESET` kept its data. The reset takes no snapshot, and session
  membership is decided by a name match, so any graph named `session_name` or
  `'{session_name}-{uuid}'` is emptied. Export or snapshot anything you need
  before the block exits, or use `STOP` to keep the data.
- A cleanup failure now raises when the `with` body itself succeeded.
  Previously a failed `DESTROY` was swallowed. When the body raised, that
  exception still wins and the cleanup failure is logged instead.
- A synchronous `with` plus `DESTROY` now waits for the deletes to finish, so
  leaving the block can take minutes. Previously the delete was not awaited.
- `STOP`, `RESET` or `DESTROY` without a `session_name` now raises
  `ValueError` at construction: with no session to scope it, the cleanup would
  target every Neptune Analytics graph in the account and region.
- `async with` is now supported, and is the recommended form in async code
  (including notebook cells). The synchronous `with` still works inside a
  running event loop, but blocks that loop — and Ctrl-C — until cleanup ends,
  and warns that it is doing so.
