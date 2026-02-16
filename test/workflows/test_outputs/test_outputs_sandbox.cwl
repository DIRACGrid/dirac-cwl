cwlVersion: v1.2
# What type of CWL process we have in this document: ComandLineTool or Workflow.
class: CommandLineTool

# The inputs for this process: none.
inputs: []
# The outputs for this process: file1
outputs:
  output1:
    type: File
    outputBinding:
      glob: "file1*"

baseCommand: ["touch", "file1"]


$namespaces:
  dirac: "../../schemas/dirac-metadata.json#/$defs/" # Generated schema from Pydantic models

$schemas:
  - "../../schemas/dirac-metadata.json"

hints:
  - class: dirac:ExecutionHooks
    hook_plugin: "QueryBasedPlugin"
    output_sandbox: ["output1"]
