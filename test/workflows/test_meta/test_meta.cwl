cwlVersion: v1.2
class: CommandLineTool

inputs: []
outputs: []

baseCommand: ["echo", "Hello World"]

$namespaces:
  dirac: "https://diracgrid.org/cwl#"

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User
    platform: x86_64
    priority: 100
    sites:
      - CTAO.DESY-ZN.de
      - CTAO.PIC.es
