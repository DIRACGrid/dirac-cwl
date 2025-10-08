cwlVersion: v1.2
class: Workflow
label: "Higher ramMin WorkflowStep"
doc: The WorkflowStep ResourceRequirement has a ramMin higher than its ramMax = failure

inputs: []
outputs: []

steps:
  bad_step:
    requirements:
      ResourceRequirement:
        ramMin: 2048 # > 1024
        ramMax: 1024
    run:
      class: CommandLineTool
      baseCommand: ["echo", "Hello World"]
      inputs: []
      outputs: []
    out: []
    in: []