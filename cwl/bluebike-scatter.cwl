cwlVersion: v1.2
class: Workflow
doc: |
  CWL-scatter encoding of the Blue-bike pipeline: the input is split into
  chunks of `unit` snapshots and the stateless stages run one task per chunk.

  Only the stateless prefix can be scattered. Change detection keeps the
  previous state of every member, the bucketiser keeps its fragment state, and
  the writer appends to a published tree, so each of those tasks must see what
  the previous one produced. CWL scatter runs its tasks independently and has
  no way to thread a value from one to the next (no fold), so the stateful
  tail runs as single tasks over the gathered stream. In a streaming system the
  same stages keep their state in memory and need no such split.
requirements:
  ScatterFeatureRequirement: {}
  SubworkflowFeatureRequirement: {}
inputs:
  snapshots: File
  mapping: File
  shape: File
  focus: File
  feedState: Directory
  unit: int
  validateStep: string
  changesStep: string
  sdsifyStep: string
  bucketizeStep: string
  writerStep: string
outputs:
  published:
    type: Directory
    outputSource: write/published
  feedStateOut:
    type: Directory
    outputSource: changes/newState
steps:
  split:
    run:
      class: CommandLineTool
      baseCommand: [split, -d, -a, '7']
      inputs:
        unit: { type: int, inputBinding: { prefix: -l, position: 1 } }
        snapshots: { type: File, inputBinding: { position: 2 } }
      arguments: [{ valueFrom: chunk_, position: 3 }]
      outputs:
        chunks:
          type: File[]
          outputBinding: { glob: chunk_* }
    in: { unit: unit, snapshots: snapshots }
    out: [chunks]
  chunk:
    run: bluebike-chunk.cwl
    scatter: snapshots
    in: { snapshots: split/chunks, mapping: mapping, validateStep: validateStep }
    out: [valid]
  gather:
    run: cat.cwl
    in: { parts: chunk/valid, name: { default: validated.ndjson } }
    out: [joined]
  changes:
    run: bb-changes.cwl
    in:
      validated: gather/joined
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
    in: { bucketed: bucketize/bucketed, metadata: bucketize/metadataOut, step: writerStep }
    out: [published]
