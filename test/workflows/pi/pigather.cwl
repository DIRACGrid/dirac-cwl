cwlVersion: v1.2
class: CommandLineTool

requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 1024

hints:
  $import: "type_dependencies/transformation/metadata-pi_gather.yaml"

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
