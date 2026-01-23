cwlVersion: v1.2
class: Workflow
label: "Monte Carlo Pi Approximation Workflow"
doc: >
  This workflow approximates the value of Pi using the Monte Carlo method.
  It generates random points in a square and calculates how many fall within
  a unit circle inscribed in the square.

# Define the inputs of the workflow
inputs:
  num-points:
    type: int
    doc: "Number of random points to generate for the simulation"
    default: 100000

# Outputs for this test are not necessary
outputs: []

# Define the steps of the workflow
#  Two independent groups of steps (simulate+gather) to force parallel
#  execution. Sequential execution time should take aproximately twice
#  the time of a parallelly executed one.
steps:
  # Group 1
  #
  simulate1:
    in:
      num-points: num-points
    out: [result_sim]
    run: ../pi/pisimulate.cwl

  gathering1:
    in:
      input-data:
        source: simulate1/result_sim
    out: [pi_result]
    run: ../pi/pigather.cwl

  # Group 2
  #
  simulate2:
    in:
      num-points: num-points
    out: [result_sim]
    run: ../pi/pisimulate.cwl

  gathering2:
    in:
      input-data:
        source: simulate2/result_sim
    out: [pi_result]
    run: ../pi/pigather.cwl
