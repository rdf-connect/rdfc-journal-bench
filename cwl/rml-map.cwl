cwlVersion: v1.2
class: CommandLineTool
doc: |
  The RML stage: rml-map (rml-processor-jvm) over an NDJSON file.
  Expects `rml-map` on PATH (cwl/bin, see bench/anchor.ts).
baseCommand: rml-map
inputs:
  records: File
  mapping:
    type: File
    inputBinding: { prefix: --mapping }
  batch:
    type: boolean
    default: true
    inputBinding: { prefix: --batch }
stdin: $(inputs.records.path)
# Named after the input: StreamFlow stages a gathered File[] into one directory,
# so outputs sharing a basename would overwrite each other there.
stdout: $(inputs.records.nameroot).mapped.ndjson
# Named explicitly: some runners (StreamFlow) otherwise merge stderr into stdout.
stderr: $(inputs.records.nameroot).map.log
outputs:
  mapped: { type: stdout }
