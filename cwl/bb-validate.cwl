cwlVersion: v1.2
class: CommandLineTool
doc: Blue-bike stage 2 — SHACL quality gate (Validate, through rdfc-proc).
baseCommand: rdfc-proc
arguments:
  - { prefix: --stdin, valueFrom: "in" }
  - { prefix: --stdout, valueFrom: "out" }
  - { prefix: --write, valueFrom: "report=$(inputs.mapped.nameroot).report.ndjson" }
inputs:
  mapped: File
  step: { type: string, inputBinding: { prefix: --config } }
stdin: $(inputs.mapped.path)
stdout: $(inputs.mapped.nameroot).valid.ndjson
stderr: $(inputs.mapped.nameroot).validate.log
outputs:
  valid: { type: stdout }
  report:
    type: File
    outputBinding: { glob: $(inputs.mapped.nameroot).report.ndjson }
