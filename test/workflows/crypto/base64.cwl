cwlVersion: v1.2
class: CommandLineTool
label: "Base64 Cipher"

requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 1024
baseCommand: ["crypto", "base64"]

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
      glob: "base64_result.txt"

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
