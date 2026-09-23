cwlVersion: v1.2
class: CommandLineTool
doc: Blue-bike stage 1 — RML mapping of station snapshots (JVM, rml-map).
baseCommand: rml-map
inputs:
  snapshots: File
  mapping: { type: File, inputBinding: { prefix: --mapping } }
stdin: $(inputs.snapshots.path)
stdout: $(inputs.snapshots.nameroot).mapped.ndjson
stderr: $(inputs.snapshots.nameroot).map.log
outputs:
  mapped: { type: stdout }
