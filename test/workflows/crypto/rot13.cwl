cwlVersion: v1.2
class: CommandLineTool
label: "ROT13 Cipher"

requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 1024
baseCommand: ["crypto", "rot13"]

inputs:
  input_string:
    type: string
    default: "Hello, World!"
    inputBinding:
      position: 1

outputs:
  output:
    type: File
    outputBinding:
      glob: "rot13_result.txt"

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
