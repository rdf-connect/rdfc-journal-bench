cwlVersion: v1.2
class: CommandLineTool
doc: |
  Blue-bike stage 5 — time-based fragmentation (Bucketize, through rdfc-proc).
  Also stateful: the fragment state is a file that must travel from task to
  task, staged in writable and handed on as an output.
requirements:
  InitialWorkDirRequirement:
    listing:
      - entry: $(inputs.state)
        entryname: bucket-state.json
        writable: true
baseCommand: rdfc-proc
arguments:
  - { prefix: --stdin, valueFrom: "in" }
  - { prefix: --stdout, valueFrom: "out" }
  - { prefix: --read, valueFrom: "metadataIn=$(inputs.metadata.path)" }
  - { prefix: --write, valueFrom: "metadataOut=$(inputs.sds.nameroot).bucketmeta.ndjson" }
inputs:
  sds: File
  metadata: File
  step: { type: string, inputBinding: { prefix: --config } }
  state: [File, "null"]
stdin: $(inputs.sds.path)
stdout: $(inputs.sds.nameroot).bucketed.ndjson
stderr: $(inputs.sds.nameroot).bucketize.log
outputs:
  bucketed: { type: stdout }
  metadataOut:
    type: File
    outputBinding: { glob: $(inputs.sds.nameroot).bucketmeta.ndjson }
  newState:
    type: File
    outputBinding: { glob: bucket-state.json }
