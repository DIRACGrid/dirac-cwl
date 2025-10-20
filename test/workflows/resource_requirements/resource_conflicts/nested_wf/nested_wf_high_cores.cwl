cwlVersion: v1.2
class: Workflow
label: "Higher NestedWorkflow resourceMin than resourceMax"
doc: This NestedWorkflow ResourceRequirement has higher coresMin than global coresMax = failure

inputs: []
outputs: []

requirements:
  ResourceRequirement:
    coresMin: 10

steps:
  step:
    run:
      class: CommandLineTool
      baseCommand: ["echo", "Hello World"]
      inputs: []
      outputs: []
    in: []
    out: []
