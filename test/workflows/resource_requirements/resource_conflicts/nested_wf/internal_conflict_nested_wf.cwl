cwlVersion: v1.2
class: Workflow
label: "Higher NestedWorkflow resourceMin than resourceMax"
doc: The NestedWorkflow ResourceRequirement has higher resourceMin than resourceMax = failure, both NestedWorflows
      here are wrong but only the first one will trigger the exception.

inputs: []
outputs: []

steps:
  too_high_ram:
    run: ../ram_conflict_wf_step.cwl
    in: [ ]
    out: [ ]
  too_high_cores:
    run: ../cores_conflict_wf_step.cwl
    in: []
    out: []
