cwlVersion: v1.2
class: Workflow
doc: One chunk of CWL-scatter, mapped and validated as a unit.
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
