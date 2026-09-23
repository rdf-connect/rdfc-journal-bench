cwlVersion: v1.2
class: CommandLineTool
doc: New stage — skolemisation (SkolemizationProcessor, through rdfc-proc).
baseCommand: rdfc-proc
arguments:
  - { prefix: --stdin, valueFrom: "in" }
  - { prefix: --stdout, valueFrom: "out" }
inputs:
  validated: File
  step: { type: string, inputBinding: { prefix: --config } }
stdin: $(inputs.validated.path)
stdout: $(inputs.validated.nameroot).skolemized.ndjson
stderr: $(inputs.validated.nameroot).skolemize.log
outputs:
  skolemized: { type: stdout }
