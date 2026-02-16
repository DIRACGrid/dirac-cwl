cwlVersion: v1.2
class: Workflow
label: "Monte Carlo Pi Approximation Workflow"
doc: >
  This workflow approximates the value of Pi using the Monte Carlo method.
  It generates random points in a square and calculates how many fall within
  a unit circle inscribed in the square.

$namespaces:
  dirac: "../../schemas/dirac-metadata.json#/$defs/"

$schemas:
  - "../../schemas/dirac-metadata.json"

# Define the inputs of the workflow
inputs:
  num-points:
    type: int
    doc: "Number of random points to generate for the simulation"
    default: 1000

# Define the outputs of the workflow
outputs:
  pi_approximation:
    type: File
    outputSource: gathering/pi_result

# Define the steps of the workflow
steps:
  # Simulation step
  simulate:
    in:
      num-points: num-points
    out: [result_sim]
    run: ./pisimulate.cwl

  # Gathering step
  gathering:
    in:
      input-data:
        source: simulate/result_sim
    out: [pi_result]
    run: ./pigather.cwl
