cwlVersion: v1.2
class: Workflow
label: "Higher NestedWorkflow resourceMin than resourceMax"
doc: The NestedWorkflow ResourceRequirement has higher resourceMin than global resourceMax = failure, both NestedWorflows
      here are wrong but only the first one will trigger the exception.

inputs: []
outputs: []

requirements:
  ResourceRequirement:
    ramMax: 512
    coresMax: 4

steps:
  too_high_ram:
    run: ./nested_wf_high_ram.cwl
    in: []
    out: []
  too_high_cores:
    run: ./nested_wf_high_cores.cwl
    in: []
    out: []
