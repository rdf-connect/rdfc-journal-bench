cwlVersion: v1.2
class: CommandLineTool
doc: Concatenates files into one named file (the gather step of CWL-scatter).
baseCommand: cat
inputs:
  parts: { type: 'File[]', inputBinding: { position: 1 } }
  name: string
stdout: $(inputs.name)
outputs:
  joined: { type: stdout }
