cwlVersion: v1.2
class: Workflow
label: "Higher WorkflowStep ramMin than Workflow ramMax"
doc: The WorkflowStep ResourceRequirement has a ramMin higher than the Workflow ResourceRequirement ramMax = failure

inputs: []
outputs: []
requirements:
  ResourceRequirement:
    ramMax: 1024 # also equals ramMin (when ramMin not specified)

steps:
  too_high_ram:
    run:
      class: CommandLineTool
      baseCommand: ["echo", "Hello World"]
      inputs: []
      outputs: []
      requirements:
        ResourceRequirement:
          ramMin: 2048 # > 1024
    in: []
    out: []