Changelog
=========

Unreleased
----------

### Behaviour changes

`SessionManager` now resolves its S3 import/export role from configuration, not
only from the caller identity. This affects existing users:

- A caller that sets `NETWORKX_S3_IAM_ROLE_ARN` or
  `nx.config.backends.neptune.s3_iam_role` for the NetworkX backend will now see
  `SessionManager` use that role too, for import, export and the permission
  preflight. Previously `SessionManager` ignored both and always used the caller
  identity. If you set the variable for the backend but intend sessions to keep
  running as the caller, pass `import_role_arn` explicitly or unset it for the
  session's process.
- Resolution order is the explicit `import_role_arn` argument, then the
  configured `s3_iam_role`, then the caller identity. STS is only consulted when
  neither of the first two is set.

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
