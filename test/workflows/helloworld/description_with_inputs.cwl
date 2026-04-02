cwlVersion: v1.2
class: CommandLineTool

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

inputs:
  message:
    type: string
    # A default value that can be overridden, e.g. --message "Hola mundo"
    default: "Hello World"
    # Bind this message value as an argument to "echo".
    inputBinding:
      position: 1
outputs: []

baseCommand: echo

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
