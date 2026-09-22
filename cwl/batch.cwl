cwlVersion: v1.2
class: Workflow
doc: |
  CWL-batch: one task per stage over the whole input, CWL's natural idiom.
  Stage 2 starts only once stage 1 has written its complete output file.
inputs:
  records: File
  mapping: File
  shapes: File
outputs:
  valid:
    type: File
    outputSource: validate/valid
  report:
    type: File
    outputSource: validate/report
steps:
  map:
    run: rml-map.cwl
    in: { records: records, mapping: mapping }
    out: [mapped]
  validate:
    run: shacl-validate.cwl
    in: { mapped: map/mapped, shapes: shapes }
    out: [valid, report]
