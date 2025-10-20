cwlVersion: v1.2
class: Workflow
label: "Higher ramMin Workflow"
doc: The Workflow ResourceRequirement has a ramMin higher than its ramMax = failure

requirements:
  ResourceRequirement:
    ramMin: 2048 # > 1024
    ramMax: 1024
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
