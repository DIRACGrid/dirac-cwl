cwlVersion: v1.2
class: Workflow
label: "Reference does not exist"
doc: >
  This workflow points to a workflow that has a circular reference.

inputs:
  input:
    type: string
    default: "Hello, World!"

outputs:
  output:
    type: File
    outputSource: bad_step/output


steps:
  bad_step:
    in:
      input: input
    out: [output]
    run: ./reference_itself.cwl

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
