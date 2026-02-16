cwlVersion: v1.2
class: CommandLineTool
baseCommand: [bash, -lc]
inputs:
  label: string
  seconds: int
outputs:
  out:
    type: File
    outputBinding:
      glob: "$(inputs.label).txt"
arguments:
  - |
    date +%s%N > "$(inputs.label).txt"
    sleep "$(inputs.seconds)"
    date +%s%N >> "$(inputs.label).txt"
