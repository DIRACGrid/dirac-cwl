cwlVersion: v1.2
class: Workflow
label: "Higher ramMin WorkflowStep.run"
doc: The WorkflowStep.run ResourceRequirement has a ramMin higher than its ramMax = failure

inputs: []
outputs: []

steps:
  bad_step:
    run:
      class: CommandLineTool
      baseCommand: ["echo", "Hello World"]
      inputs: []
      outputs: []
      requirements:
        ResourceRequirement:
          ramMin: 2048 # > 1024
          ramMax: 1024
    out: []
    in: []