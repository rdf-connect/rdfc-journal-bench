cwlVersion: v1.2
class: CommandLineTool
doc: |
  Blue-bike stage 6 — publication as a static LDES on disk (LdesDiskWriter).
  The published tree is state as well: it is staged in and handed on, so that
  successive tasks append to the same LDES.
requirements:
  InitialWorkDirRequirement:
    listing:
      - entry: $(inputs.ldes)
        entryname: ldes-output
        writable: true
baseCommand: rdfc-proc
arguments:
  - { prefix: --stdin, valueFrom: "in" }
  - { prefix: --read, valueFrom: "metadataIn=$(inputs.metadata.path)" }
inputs:
  bucketed: File
  metadata: File
  step: { type: string, inputBinding: { prefix: --config } }
  ldes: [Directory, "null"]
stdin: $(inputs.bucketed.path)
stderr: $(inputs.bucketed.nameroot).writer.log
outputs:
  published:
    type: Directory
    outputBinding: { glob: ldes-output }
