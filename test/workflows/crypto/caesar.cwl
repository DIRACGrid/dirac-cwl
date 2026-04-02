cwlVersion: v1.2
class: CommandLineTool
label: "Caesar Cipher"

requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 1024
baseCommand: ["crypto", "caesar"]

inputs:
  input_string:
    type: string
    default: "Hello, World!"
    inputBinding:
      position: 1
  shift_value:
    type: int
    default: 3
    inputBinding:
      position: 2

outputs:
  output:
    type: File
    outputBinding:
      glob: "caesar_result.txt"

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
