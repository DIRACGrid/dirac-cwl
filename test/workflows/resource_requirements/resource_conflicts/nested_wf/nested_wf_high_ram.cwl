cwlVersion: v1.2
class: Workflow
label: "Higher NestedWorkflow resourceMin than resourceMax"
doc: This NestedWorkflow ResourceRequirement has higher ramMin than global ramMax = failure


inputs: []
outputs: []

requirements:
  ResourceRequirement:
    ramMin: 1028

steps:
  step:
    run:
      class: CommandLineTool
      baseCommand: ["echo", "Hello World"]
      inputs: []
      outputs: []
    in: []
    out: []
