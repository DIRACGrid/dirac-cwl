cwlVersion: v1.2
class: Workflow
label: "Higher WorkflowStep coresMin than Workflow coresMax"
doc: The WorkflowStep ResourceRequirement has a coresMin higher than the Workflow ResourceRequirement coresMax = failure

inputs: []
outputs: []
requirements:
  ResourceRequirement:
    coresMax: 2 # also equals coresMin (when coresMin not specified)

steps:
  too_high_cores:
    requirements:
      ResourceRequirement:
        coresMin: 4 # > 2
    run:
      class: CommandLineTool
      baseCommand: ["echo", "Hello World"]
      inputs: []
      outputs: []
    in: []
    out: []
