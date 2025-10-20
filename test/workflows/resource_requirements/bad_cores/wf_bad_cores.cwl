cwlVersion: v1.2
class: Workflow
label: "Higher coresMin Workflow"
doc: The Workflow ResourceRequirement has a coresMin higher than its coresMax = failure

requirements:
  ResourceRequirement:
    coresMin: 4 # > 2
    coresMax: 2
inputs: []
outputs: []

steps:
  good_step:
    run:
      class: CommandLineTool
      baseCommand: ["echo", "Hello World"]
      inputs: []
      outputs: []
    out: []
    in: []
