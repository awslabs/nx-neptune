Changelog
=========

0.9.0 (unreleased)
------------------

Behavior changes:

- Graphs created by nx-neptune are tagged `nx-neptune:managed=true`. Destroy and
  reset only act on graphs with this tag, whichever API is used. In `SessionManager`,
  bulk calls skip other graphs with a warning, and calls that name a graph raise
  `ValueError`.
- Graphs created by earlier versions only carry `agent=nx-neptune`. They are still
  treated as managed for now. Support for this fallback will be removed in a later
  release. To keep managing such a graph, add the new tag:
  `aws neptune-graph tag-resource --resource-arn <arn> --tags nx-neptune:managed=true`.
- The `agent` and `nx-neptune:managed` tags are always set to the library's values.
  A caller-supplied `agent` tag is overwritten. Other caller tags are kept.
- `delete_na_instance`, and NetworkX teardown with `destroy_instance=True`, only
  delete graphs created by nx-neptune, and raise `ValueError` for other graphs.
  Deletion protection is only cleared on graphs created by nx-neptune.
- `reset_graph`, and `import_csv_from_s3` with `reset_graph_ahead=True`, only reset
  graphs created by nx-neptune, and raise `ValueError` for other graphs. Pass
  `allow_unmanaged=True` to reset another graph. The NetworkX backend does this
  for imports into a graph provided through `graph_id`. `reset_graph` also checks
  IAM permissions; pass `check_permissions=False` to skip the check.
- NetworkX backend: `destroy_instance=True` is only supported with
  `create_new_instance`. Setting it together with `graph_id` (or
  `NETWORKX_GRAPH_ID`) is a configuration error, so a graph provided by the
  caller is never destroyed.
- Destroy and reset need `neptune-graph:GetGraph` and
  `neptune-graph:ListTagsForResource`. The CloudFormation template grants both.
