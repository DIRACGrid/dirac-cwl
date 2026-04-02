cwlVersion: v1.2
class: CommandLineTool

inputs: []
outputs: []

# baseComand instead of baseCommand
baseComand: malformedcommand

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
