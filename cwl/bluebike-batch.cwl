cwlVersion: v1.2
class: Workflow
doc: |
  CWL-batch encoding of the Blue-bike pipeline: one task per stage over the
  whole input, which is how a CWL user would write it. Each stage starts only
  once its predecessor has written its complete output file.
inputs:
  snapshots: File
  mapping: File
  shape: File
  focus: File
  feedState: Directory
  # A scheduled re-run appends to what it published last time.
  ldes: [ "null", Directory ]
  validateStep: string
  changesStep: string
  sdsifyStep: string
  bucketizeStep: string
  writerStep: string
outputs:
  published:
    type: Directory
    outputSource: write/published
  report:
    type: File
    outputSource: validate/report
  feedStateOut:
    type: Directory
    outputSource: changes/newState
steps:
  map:
    run: bb-map.cwl
    in: { snapshots: snapshots, mapping: mapping }
    out: [mapped]
  validate:
    run: bb-validate.cwl
    in: { mapped: map/mapped, step: validateStep }
    out: [valid, report]
  changes:
    run: bb-changes.cwl
    in:
      validated: validate/valid
      step: changesStep
      shape: shape
      focus: focus
      state: feedState
    out: [feed, newState]
  sdsify:
    run: bb-sdsify.cwl
    in: { feed: changes/feed, step: sdsifyStep }
    out: [sds, metadata]
  bucketize:
    run: bb-bucketize.cwl
    in: { sds: sdsify/sds, metadata: sdsify/metadata, step: bucketizeStep }
    out: [bucketed, metadataOut, newState]
  write:
    run: bb-writer.cwl
    in:
      bucketed: bucketize/bucketed
      metadata: bucketize/metadataOut
      step: writerStep
      ldes: ldes
    out: [published]
