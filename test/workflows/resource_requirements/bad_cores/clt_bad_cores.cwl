cwlVersion: v1.2
class: CommandLineTool
label: "Higher coresMin CLT"
doc: The CommandLineTool ResourceRequirement has a coresMin higher than its coresMax = failure

requirements:
  ResourceRequirement:
    coresMin: 4 # > 2
    coresMax: 2
inputs: []
outputs: []

baseCommand: ["echo", "Hello World"]
