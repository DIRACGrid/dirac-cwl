cwlVersion: v1.2
# New class is incorrect
class: NewClass

inputs: []
outputs: []

baseCommand: ["echo", "Hello World"]

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
