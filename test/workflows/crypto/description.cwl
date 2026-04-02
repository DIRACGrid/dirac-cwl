cwlVersion: v1.2
class: Workflow
label: "CryptoFun Workflow"
doc: >
  This workflow demonstrates basic cryptographic transformations. It takes a string input and processes it through four independent CommandLineTool steps:
  a Caesar cipher, Base64 encoding, MD5 hashing, and ROT13 encryption.

inputs:
  input_string:
    type: string
    default: "Hello, World!"
    doc: "The input string to encrypt"
  shift_value:
    type: int
    default: 3
    doc: "The Caesar cipher shift value"

outputs:
  caesar_output:
    type: File
    outputSource: caesar_step/output
    doc: "Caesar cipher output"
  base64_output:
    type: File
    outputSource: base64_step/output
    doc: "Base64 encoded output"
  md5_output:
    type: File
    outputSource: md5_step/output
    doc: "MD5 hash output"
  rot13_output:
    type: File
    outputSource: rot13_step/output
    doc: "ROT13 encrypted output"

steps:
  caesar_step:
    in:
      input_string: input_string
      shift_value: shift_value
    out: [output]
    run: ./caesar.cwl

  base64_step:
    in:
      input_string: input_string
    out: [output]
    run: ./base64.cwl

  md5_step:
    in:
      input_string: input_string
    out: [output]
    run: ./md5.cwl


  rot13_step:
    in:
      input_string: input_string
    out: [output]
    run: ./rot13.cwl

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
