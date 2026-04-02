cwlVersion: v1.2
class: CommandLineTool

requirements:
  ResourceRequirement:
    coresMin: 2
    ramMin: 1024

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

$namespaces:
  dirac: "https://diracgrid.org/cwl#"

inputs:
  num-points:
    type: int
    default: 1000
    inputBinding:
      position: 1

outputs:
  result_sim:
    type: File[]
    outputBinding:
      glob: "result*.sim"

baseCommand: [ pi-simulate ]
