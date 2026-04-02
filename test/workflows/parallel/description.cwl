cwlVersion: v1.2
class: Workflow

inputs:
  seconds:
    type: int
    default: 3

outputs:
  a_out:
    type: File
    outputSource: step_a/out
  b_out:
    type: File
    outputSource: step_b/out

steps:
  step_a:
    run: sleep.cwl
    in:
      label: { default: "a" }
      seconds: seconds
    out: [out]

  step_b:
    run: sleep.cwl
    in:
      label: { default: "b" }
      seconds: seconds
    out: [out]


hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
