cwlVersion: v1.2
class: Workflow
label: "Reference does not exist"
doc: >
  This workflow points to a non-existent CWL file.

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
    run: ./doesnotexist.cwl

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
