cwlVersion: v1.2
class: Workflow
doc: "One chunk of the scattered prefix: map, then validate."
inputs:
  snapshots: File
  mapping: File
  validateStep: string
outputs:
  valid:
    type: File
    outputSource: validate/valid
steps:
  map:
    run: bb-map.cwl
    in: { snapshots: snapshots, mapping: mapping }
    out: [mapped]
  validate:
    run: bb-validate.cwl
    in: { mapped: map/mapped, step: validateStep }
    out: [valid, report]
