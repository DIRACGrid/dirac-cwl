cwlVersion: v1.2
class: CommandLineTool

requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 1024

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

$namespaces:
  dirac: "https://diracgrid.org/cwl#"

inputs:
  input-data:
    type: File[]
    inputBinding:
      separate: true

outputs:
  pi_result:
    type: File
    outputBinding:
      glob: "result*.sim"

baseCommand: [ pi-gather ]
