cwlVersion: v1.2
class: Workflow
label: "Higher coresMin WorkflowStep"
doc: The WorkflowStep ResourceRequirement has a coresMin higher than its coresMax = failure

inputs: []
outputs: []

steps:
  bad_step:
    requirements:
      ResourceRequirement:
        coresMin: 4 # > 2
        coresMax: 2
    run:
      class: CommandLineTool
      baseCommand: ["echo", "Hello World"]
      inputs: []
      outputs: []
    out: []
    in: []
