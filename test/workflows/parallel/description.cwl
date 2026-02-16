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
  - class: dirac:ExecutionHooks
    hook_plugin: "QueryBasedPlugin"
    output_sandbox: ["a_out", "b_out"]


$namespaces:
  dirac: "../../schemas/dirac-metadata.json#/$defs/" # Generated schema from Pydantic models

$schemas:
  - "../../schemas/dirac-metadata.json"
