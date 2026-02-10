cwlVersion: v1.2
class: Workflow
doc: >
  Currently used with pi workflows since they have input-data inputs files.
  Count the number of files in the inputs file of the current job group and list them.
  This workflow is used to test the Automatic Job Grouping when doing a Transformation Job.
  From one file, several jobs will be created and the values in the inputs file will be splitted among them.

requirements:
  InlineJavascriptRequirement: {}

inputs:
  input-data:
    type: { type: array, items: [ string, File ] }

outputs:
  result-file:
    type: File
    outputSource: count/result

hints:
  - class: dirac:ExecutionHooks
    group_size: 2 # will create nb_inputs // 2 jobs
    output_sandbox: ["result-file"]

steps:
  count:
    in:
      files: input-data
    out: [result]
    run:
      class: CommandLineTool
      baseCommand: echo
      inputs:
        files:
          type: { type: array, items: [ string, File ] }
      arguments:
        - "The number of files in this group is:"
        - $(inputs.files.length)
        - "\nFiles in this group:"
        - |
          ${
            let result = "";
            for (let i = 0; i < inputs.files.length; i++) {
                let f = inputs.files[i];
                let name = (typeof f === 'string') ? f : (f.path || f.location).split('/').pop();
                result += "\n- " + name;
            }
            return result;
          }

      stdout: output.txt

      outputs:
        result:
          type: stdout
