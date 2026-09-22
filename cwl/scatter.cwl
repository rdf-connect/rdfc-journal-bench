cwlVersion: v1.2
class: Workflow
doc: |
  CWL-scatter: CWL contorted into a streaming shape. The input is split into
  chunks of `unit` records, every chunk runs as its own map → validate
  subworkflow (one process per stage per chunk), and the results are
  concatenated. CWL never splits files itself, so split and gather are
  explicit steps, inside the measured workflow.
requirements:
  ScatterFeatureRequirement: {}
  SubworkflowFeatureRequirement: {}
inputs:
  records: File
  mapping: File
  shapes: File
  unit: int
outputs:
  valid:
    type: File
    outputSource: gather/joined
  report:
    type: File
    outputSource: gather_reports/joined
steps:
  split:
    run:
      class: CommandLineTool
      baseCommand: [split, -d, -a, '7']
      inputs:
        unit: { type: int, inputBinding: { prefix: -l, position: 1 } }
        records: { type: File, inputBinding: { position: 2 } }
      arguments: [{ valueFrom: chunk_, position: 3 }]
      outputs:
        chunks:
          type: File[]
          outputBinding: { glob: chunk_* }
    in: { unit: unit, records: records }
    out: [chunks]
  chunk:
    run: chunk.cwl
    scatter: records
    in: { records: split/chunks, mapping: mapping, shapes: shapes }
    out: [valid, report]
  gather:
    run: cat.cwl
    in: { parts: chunk/valid, name: { default: valid.ndjson } }
    out: [joined]
  gather_reports:
    run: cat.cwl
    in: { parts: chunk/report, name: { default: report.ndjson } }
    out: [joined]
