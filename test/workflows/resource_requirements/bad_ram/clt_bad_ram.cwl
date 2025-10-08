cwlVersion: v1.2
class: CommandLineTool
label: "Higher ramMin CLT"
doc: The CommandLineTool ResourceRequirement has a ramMin higher than its ramMax = failure

requirements:
  ResourceRequirement:
    ramMin: 2048 # > 1024
    ramMax: 1024
inputs: []
outputs: []

baseCommand: ["echo", "Hello World"]