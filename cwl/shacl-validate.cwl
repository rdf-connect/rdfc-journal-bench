cwlVersion: v1.2
class: CommandLineTool
doc: |
  The SHACL stage: shacl-validate (shacl-processor-ts) over an NDJSON file.
  Like the Validate processor it has two outputs: the data if the unit
  conforms in full, and the validation report if it does not.
  Expects `shacl-validate` on PATH (cwl/bin, see bench/anchor.ts).
baseCommand: shacl-validate
arguments: [--report, $(inputs.mapped.nameroot).report.ndjson]
inputs:
  mapped: File
  shapes:
    type: File
    inputBinding: { prefix: --shapes }
  batch:
    type: boolean
    default: true
    inputBinding: { prefix: --batch }
stdin: $(inputs.mapped.path)
# Named after the input, for the same reason as in rml-map.cwl.
stdout: $(inputs.mapped.nameroot).valid.ndjson
# Named explicitly: some runners (StreamFlow) otherwise merge stderr into stdout.
stderr: $(inputs.mapped.nameroot).validate.log
outputs:
  valid: { type: stdout }
  report:
    type: File
    outputBinding: { glob: $(inputs.mapped.nameroot).report.ndjson }
