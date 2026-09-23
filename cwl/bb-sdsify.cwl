cwlVersion: v1.2
class: CommandLineTool
doc: Blue-bike stage 4 — SDS annotation (Sdsify, through rdfc-proc).
baseCommand: rdfc-proc
arguments:
  - { prefix: --stdin, valueFrom: "in" }
  - { prefix: --stdout, valueFrom: "out" }
  - { prefix: --write, valueFrom: "metadata=$(inputs.feed.nameroot).sdsmeta.ndjson" }
inputs:
  feed: File
  step: { type: string, inputBinding: { prefix: --config } }
stdin: $(inputs.feed.path)
stdout: $(inputs.feed.nameroot).sds.ndjson
stderr: $(inputs.feed.nameroot).sdsify.log
outputs:
  sds: { type: stdout }
  metadata:
    type: File
    outputBinding: { glob: $(inputs.feed.nameroot).sdsmeta.ndjson }
