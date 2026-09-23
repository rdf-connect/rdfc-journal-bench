# Change effort: adding a stage

The definitions before and after inserting one published processor
(`SkolemizationProcessor`, replacing blank nodes with IRIs) between the quality
gate and change detection, in the Blue-bike pipeline.

| Arm | Diff | Files touched |
|---|---:|---|
| RDF-Connect | 10 lines in `pipeline.ttl` (import, runner list, channel, processor) | 1 |
| shell | 1 line in the pipe, plus the 9-line step description | 2 |
| CWL | 7 lines in the workflow, plus a new 15-line tool, plus the 9-line step description | 3 |

The step description (`steps/skolemize.ttl`) is what the CLI arms need to
configure the processor; the RDF-Connect pipeline carries that configuration
inline, which is why its single document is the whole change.

Both modified definitions were checked, not just counted: the RDF-Connect
pipeline runs and publishes exactly the expected members with the stage added,
and the CWL workflow validates.

Files here are the measured artefacts:

- `rdfc-before.ttl` / `rdfc-after.ttl` — the generated pipeline, before and after
- `cwl-before.cwl` / `cwl-after.cwl` — the batch workflow, before and after
- `cwl-after-tool.cwl` — the tool description the new stage needs in CWL
