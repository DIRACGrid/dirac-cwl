cwlVersion: v1.2
class: CommandLineTool

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

inputs: []
outputs: []

baseCommand: ["echo", "Hello World"]

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
