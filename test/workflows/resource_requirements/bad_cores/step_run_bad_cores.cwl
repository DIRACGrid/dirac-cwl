cwlVersion: v1.2
class: Workflow
label: "Higher coresMin WorkflowStep.run"
doc: The WorkflowStep.run ResourceRequirement has a coresMin higher than its coresMax = failure

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
          coresMin: 4 # > 2
          coresMax: 2
    out: []
    in: []