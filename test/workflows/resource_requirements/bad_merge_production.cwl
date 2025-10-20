cwlVersion: v1.2
label: "Bad Merge Workflow"
class: Workflow
doc: The global ResourceRequirement should not be allowed in a production workflow, file depends on "test/workflows/merge" folder

requirements:
  MultipleInputFeatureRequirement: {}
  ResourceRequirement: # not allowed here
    coresMax: 4

# Define the inputs of the workflow
inputs:
  num-points:
    type: int
    doc: "Number of random points to generate for the simulation"
    default: 1000
  output-path-step1:
    type: string
    default: result_1.sim
  output-path-step2:
    type: string
    default: result_2.sim

# Define the outputs of the workflow
outputs:
  pi_approximation:
    type: File
    outputSource: gathering/pi_result

# Define the steps of the workflow
steps:
  # Simulation step 1
  simulate_step1:
    in:
      num-points: num-points
      output-path: output-path-step1
    out: [sim]
    run: ../merge/pisimulate_v2.cwl

  # Simulation step 2
  simulate_step2:
    in:
      num-points: num-points
      output-path: output-path-step2
    out: [sim]
    run: ../merge/pisimulate_v2.cwl

  # Gathering step
  gathering:
    in:
      input-data:
        source:
          - simulate_step1/sim
          - simulate_step2/sim
        linkMerge: merge_flattened
    out: [pi_result]
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 1
          ramMin: 1024
      inputs:
        input-data:
          type: File[]
          inputBinding:
            separate: true
      outputs:
        pi_result:
          type: File
          outputBinding:
            glob: "result*.sim"
      baseCommand: [pi-gather]
