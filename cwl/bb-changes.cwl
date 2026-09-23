cwlVersion: v1.2
class: CommandLineTool
doc: |
  Blue-bike stage 3 — change detection (DumpsToFeed, through rdfc-proc).
  The stage is stateful: it keeps the previous state of every member in a
  LevelDB. In a stream that state simply lives on; here it has to be staged in
  as a directory and handed on as an output, task by task.
requirements:
  InitialWorkDirRequirement:
    listing:
      - entry: $(inputs.state)
        entryname: feed-state
        writable: true
baseCommand: rdfc-proc
arguments:
  - { prefix: --stdin, valueFrom: "in" }
  - { prefix: --stdout, valueFrom: "out" }
  - { prefix: --read, valueFrom: "shape=$(inputs.shape.path)" }
  - { prefix: --read, valueFrom: "focusNodes=$(inputs.focus.path)" }
inputs:
  validated: File
  step: { type: string, inputBinding: { prefix: --config } }
  shape: File
  focus: File
  state: Directory
stdin: $(inputs.validated.path)
stdout: $(inputs.validated.nameroot).feed.ndjson
stderr: $(inputs.validated.nameroot).changes.log
outputs:
  feed: { type: stdout }
  newState:
    type: Directory
    outputBinding: { glob: feed-state }
