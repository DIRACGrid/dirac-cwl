# Changelog

## [1.1.3](https://github.com/DIRACGrid/dirac-cwl/compare/v1.1.2...v1.1.3) (2026-02-17)


### Bug Fixes

* **executor:** run pure_python hook before ANY cwltool import ([#120](https://github.com/DIRACGrid/dirac-cwl/issues/120)) ([747a4b2](https://github.com/DIRACGrid/dirac-cwl/commit/747a4b239288ed78987b2179ee8802ea5d27ba8c))

## [1.1.2](https://github.com/DIRACGrid/dirac-cwl/compare/v1.1.1...v1.1.2) (2026-02-17)


### Bug Fixes

* move mypy to optional-deps ([#118](https://github.com/DIRACGrid/dirac-cwl/issues/118)) ([d59d4c0](https://github.com/DIRACGrid/dirac-cwl/commit/d59d4c04a7f1d1fe9a47f4ebcd908c95e2d42d64))

## [1.1.1](https://github.com/DIRACGrid/dirac-cwl/compare/v1.1.0...v1.1.1) (2026-02-16)


### Bug Fixes

* remove library-level basicConfig and clean up logging ([#113](https://github.com/DIRACGrid/dirac-cwl/issues/113)) ([5c8f200](https://github.com/DIRACGrid/dirac-cwl/commit/5c8f2008d9c91212d920fc062a8ed1cec48b214d))

## [1.1.0](https://github.com/DIRACGrid/dirac-cwl/compare/v1.0.2...v1.1.0) (2026-02-06)


### Features

* CWL Executor and hint design for input data ([#94](https://github.com/DIRACGrid/dirac-cwl/issues/94)) ([e658633](https://github.com/DIRACGrid/dirac-cwl/commit/e658633a6e4d33dcebce4526bec55dd4b5d3ea14))

## [1.0.2](https://github.com/DIRACGrid/dirac-cwl/compare/v1.0.1...v1.0.2) (2026-02-05)


### Bug Fixes

* don't do a shallow clone in order to see newly created tags ([0100427](https://github.com/DIRACGrid/dirac-cwl/commit/0100427bd8072cc3041d08d7b05f3bb873abc761))

## [1.0.1](https://github.com/DIRACGrid/dirac-cwl/compare/v1.0.0...v1.0.1) (2026-02-05)


### Bug Fixes

* diracx-cwl --&gt; dirac-cwl ([52224c5](https://github.com/DIRACGrid/dirac-cwl/commit/52224c55a5507fb0fbd9aa5c6c4522b56dfe0407))

## 1.0.0 (2026-02-05)


### Features

* "mock" DIRAC DataManager ([556e3c5](https://github.com/DIRACGrid/dirac-cwl/commit/556e3c51f7d008a75617e9d84e77ce4e6811a7cf))
* add a SandboxStore like client ([f1c1143](https://github.com/DIRACGrid/dirac-cwl/commit/f1c1143e131c5b293c84df8c441bce7481b403c9))
* add a submit_job CLI ([48a50b4](https://github.com/DIRACGrid/dirac-cwl/commit/48a50b46cecc558d0f515e4a5c9c5dd0e990536e))
* add commands in pyprojet.toml ([2be7c18](https://github.com/DIRACGrid/dirac-cwl/commit/2be7c1846e4b43632b3ac5d688605f73020daa9c))
* add cwltool usage in README ([5fb88f3](https://github.com/DIRACGrid/dirac-cwl/commit/5fb88f38fe714c5f29685eb9228b36e830244417))
* add further tests for referenced workflows ([e8217a1](https://github.com/DIRACGrid/dirac-cwl/commit/e8217a1d687355a72f8e563ba5aadd46f25e05b8))
* add gaudi app ([cfdfb18](https://github.com/DIRACGrid/dirac-cwl/commit/cfdfb18e4e60025eeff7daaa16438ecabec6467a))
* Add hints to pre-process and post-process jobs depending on its job type ([b693905](https://github.com/DIRACGrid/dirac-cwl/commit/b693905cab0c2fd8306e089c6d26e1cfeca22f58))
* Add hints to pre-process and post-process jobs depending on its job type ([ee7683a](https://github.com/DIRACGrid/dirac-cwl/commit/ee7683aea90450a0a922fbea3e441f6f619bafcf))
* add lfns_input to job input model ([5c8ba41](https://github.com/DIRACGrid/dirac-cwl/commit/5c8ba4117899ec4d78f0e4846a8707b2042c028d))
* add lhcb_app_v2 and lhcb_workflow_v2 ([bdd3a7f](https://github.com/DIRACGrid/dirac-cwl/commit/bdd3a7f818e5f8839f6b041abc38f131754a978d))
* add lhcb_app_v3 and lhcb_workflow_v3 ([69430e5](https://github.com/DIRACGrid/dirac-cwl/commit/69430e5bbca197bbf3ac4284efd720753146d96e))
* add metadata models ([c1eb3a4](https://github.com/DIRACGrid/dirac-cwl/commit/c1eb3a4078393852f5a3daa5155f82126e5dd570))
* add output_paths and output_sandbox hints ([07fe25d](https://github.com/DIRACGrid/dirac-cwl/commit/07fe25d7f3c2b914751ae9b4ca3b5ce5f863046b))
* add output_se hint ([6d2ad79](https://github.com/DIRACGrid/dirac-cwl/commit/6d2ad79e9e862db7b0bd8b59fc0ef4fa11099a1e))
* Add parallel workflow execution ([bd2e9f1](https://github.com/DIRACGrid/dirac-cwl/commit/bd2e9f12243bf969ec1647cc49b3fd73bd074699))
* add structure ([a186393](https://github.com/DIRACGrid/dirac-cwl/commit/a186393e0036fbd7d6a82838605239335b0e7cfb))
* almost a first minimal implementation ([e41e252](https://github.com/DIRACGrid/dirac-cwl/commit/e41e252410b7ca8c058bc4a5ff6c4f339c8f84f1))
* created MockDiracXSandboxAPI to use diracx sandbox upload and download instead of DIRAC ([84f6053](https://github.com/DIRACGrid/dirac-cwl/commit/84f60537fd8b9b04e0e6cefaac3d6597ae23376e))
* download lfns in the execution hook ([edc2182](https://github.com/DIRACGrid/dirac-cwl/commit/edc2182be88e22292c3a070721c3406f7fd96563))
* **ExecutionHooksPluginRegistry:** Move ExecutionHooksPlugin discovery to pyproject entrypoints ([94e8a9b](https://github.com/DIRACGrid/dirac-cwl/commit/94e8a9b0bba502eea63e59d8d9bcf26dc8e7e88f))
* **ExecutionHooksPluginRegistry:** Move ExecutionHooksPlugin discovery to pyproject entrypoints ([b003d19](https://github.com/DIRACGrid/dirac-cwl/commit/b003d19d39f67bda51a45ae8bf0c3fefa2d2c68e))
* first minimal working version ([f8c9412](https://github.com/DIRACGrid/dirac-cwl/commit/f8c94125640ce07a4ba4655bc69aa506fdb46f6f))
* first version of lhcb cwl ([264f472](https://github.com/DIRACGrid/dirac-cwl/commit/264f472878c1c5ed9b3596c746a2a9f2e1f96666))
* initialize Dirac for real job inputs and outputs ([f5a88ce](https://github.com/DIRACGrid/dirac-cwl/commit/f5a88cecad334bc429fdc980ad8586698d3e6477))
* **Job Endpoint:** add job output data ([effcc9a](https://github.com/DIRACGrid/dirac-cwl/commit/effcc9ae7233d4523598613db1d6e2fb6965b185))
* **Job Endpoint:** add job output sandboxes ([05bd754](https://github.com/DIRACGrid/dirac-cwl/commit/05bd75443079c9059c83645f11fefe0d93d0c513))
* **Job Endpoint:** add job submission with input data using LFN prefix ([fc60245](https://github.com/DIRACGrid/dirac-cwl/commit/fc602451650d85847700a7f6841b860f1f09b6cd))
* **Job Endpoint:** move job output management to Execution Hooks ([82de6e7](https://github.com/DIRACGrid/dirac-cwl/commit/82de6e7ff240e20365991532c84f3fca2a03611e))
* job interface ([da827b1](https://github.com/DIRACGrid/dirac-cwl/commit/da827b11d53f9e86219d751e0ef497531e9f0bef))
* lfns_output_overrides field ([2f9df75](https://github.com/DIRACGrid/dirac-cwl/commit/2f9df7517dcda569d56e1e1cb3448cd1221ad90f))
* log post-process failure ([d760ffd](https://github.com/DIRACGrid/dirac-cwl/commit/d760ffd4e3ededa18ea6dfa479e14e4e3ffec151))
* mimic input data downloading ([5231dd4](https://github.com/DIRACGrid/dirac-cwl/commit/5231dd4ad26f4d4b54bbec8e1a41f852154573c0))
* mimic input sandbox and crypto example ([110ce80](https://github.com/DIRACGrid/dirac-cwl/commit/110ce80d12520eb77939ac1bba4aac0d79c39684))
* mocks following DIRAC's definition ([44ae43a](https://github.com/DIRACGrid/dirac-cwl/commit/44ae43a5cc3fcdea283b81dfc4733fab06cef7c9))
* move output_query and input_query to QueryBasedPlugin and clean code ([da46de9](https://github.com/DIRACGrid/dirac-cwl/commit/da46de9484695516317c471c538ed30b32998514))
* Move pre and post processing commands execution from 'JobProcessorBase' to 'ExecutionHooksBasePlugin' using specialized classes instead of 'CommandBase' ([44669ef](https://github.com/DIRACGrid/dirac-cwl/commit/44669efb12e50a164ac44c39cb42ef3adcfaba1a))
* new interfaces ([79eb100](https://github.com/DIRACGrid/dirac-cwl/commit/79eb100e9c06a5cee056a6de228b23fbd963dfe8))
* new job wrapper DIRAC Integration ([e33ff32](https://github.com/DIRACGrid/dirac-cwl/commit/e33ff32592221bc4cfe60c16b03fe52f57ff4929))
* new structure ([5260a7a](https://github.com/DIRACGrid/dirac-cwl/commit/5260a7a81a756da8a05b380d7153a4030a0cfe2b))
* partially add mandelbrot workflow ([e2d3bb4](https://github.com/DIRACGrid/dirac-cwl/commit/e2d3bb425c0221bfd9ada7506806ac8d59af29d7))
* pathless baseuri in cwl ids ([602a542](https://github.com/DIRACGrid/dirac-cwl/commit/602a5428eec798c3498d7286de90b6886f0bd7bc))
* possibility to specify Path with 'path' key in inputs ([0a5bc9c](https://github.com/DIRACGrid/dirac-cwl/commit/0a5bc9c7df04f7222ecc18c623de0e10f69ab1a1))
* prepare cli to work with lhcb workflows (1st draft) ([c6cff08](https://github.com/DIRACGrid/dirac-cwl/commit/c6cff08a1b12794a2ae34c6a17fc1b5865b8ba88))
* production interface ([a73f479](https://github.com/DIRACGrid/dirac-cwl/commit/a73f4797dfc3465400008deaf3e5576dfe7360b3))
* publish json schemas ([7405ffa](https://github.com/DIRACGrid/dirac-cwl/commit/7405ffaedca270d96a43b30d2bf22acdc42c3317))
* publish json schemas ([6929d89](https://github.com/DIRACGrid/dirac-cwl/commit/6929d89a26205e57abb9d2023a9c0a297c9b05bc))
* pypi deployment with release-please ([#96](https://github.com/DIRACGrid/dirac-cwl/issues/96)) ([bdbcf5e](https://github.com/DIRACGrid/dirac-cwl/commit/bdbcf5e76c2bfdcd5770a2e4f3ef55892655c8f1))
* Raise special exception for pre/post processing commands ([c5c1285](https://github.com/DIRACGrid/dirac-cwl/commit/c5c12855312db52d9a887f4c88df3f34fd7b4b51))
* reintroduce the generic workflow ([57fc087](https://github.com/DIRACGrid/dirac-cwl/commit/57fc08772ff3cd8d4e98f95396aae2938708ae44))
* sandbox store using diracx api mock ([36b6654](https://github.com/DIRACGrid/dirac-cwl/commit/36b66542e3b7505970072b75e2fe642f8b6fdeba))
* split code into 3 modules (job, trans, prod) ([b3033c4](https://github.com/DIRACGrid/dirac-cwl/commit/b3033c4b0a8708a72b9eb0539e9f0f5ca71b8e1b))
* streamline workflows and tests ([412b7e8](https://github.com/DIRACGrid/dirac-cwl/commit/412b7e8724c396d8fd474ee5787407a2103edfa1))
* **test:** make lhcb workflow executable locally ([c8b7cf8](https://github.com/DIRACGrid/dirac-cwl/commit/c8b7cf8f178470594b828fe874a0a35b645a0c6c))
* **test:** make lhcb workflow executable locally ([452c0d2](https://github.com/DIRACGrid/dirac-cwl/commit/452c0d21be6c419125975d05cf1163e4a4350e28))
* **tests:** Extend and adapt ProPostProcessingHint tests ([820d115](https://github.com/DIRACGrid/dirac-cwl/commit/820d115320a34940f8a94b5b07d8d2d18484ea1c))
* transformation interface ([4211223](https://github.com/DIRACGrid/dirac-cwl/commit/4211223c12d6051d8de2511b57e62bead3f5e9bd))
* use DataManager and SandboxStore in job pre-process ([e599f9b](https://github.com/DIRACGrid/dirac-cwl/commit/e599f9b20d02999e8ebe56879323c8e74647ff01))
* use DIRAC FileStorage ([e8acb1a](https://github.com/DIRACGrid/dirac-cwl/commit/e8acb1abbeed3afb1f131123ccdb7ea42245ba89))
* use location instead of path for lfns ([aa06d3c](https://github.com/DIRACGrid/dirac-cwl/commit/aa06d3cc0034dddcfff89609874b0d8b4cc1f395))
* **workflows:** reference subworkflows in main workflows ([a8d9fff](https://github.com/DIRACGrid/dirac-cwl/commit/a8d9fff1d421ee0291bab801811c7b24805072da))
* **workflows:** reference subworkflows in main workflows to avoid repeating them ([d77bed8](https://github.com/DIRACGrid/dirac-cwl/commit/d77bed85b373eb2a8018e96c190673351ec6f5c2))


### Bug Fixes

* add job submission itself into the input sandbox when submitting to DIRAC ([25bfa4c](https://github.com/DIRACGrid/dirac-cwl/commit/25bfa4c2e83986778bec21d4bd6ed12cf5636e86))
* added real SandboxInfo from diracx instead of a Mock ([c7c73ec](https://github.com/DIRACGrid/dirac-cwl/commit/c7c73ecdcb5878c5569d9bd622865e3f26dd3d3c))
* apply pr suggestions and add some docstring ([924420a](https://github.com/DIRACGrid/dirac-cwl/commit/924420a7f36202019f07b7834a68b7221351e457))
* better displaying errors coming from job execution ([dbe2654](https://github.com/DIRACGrid/dirac-cwl/commit/dbe2654581a0de70d86a071da2b9242702d051c4))
* change prefix ([240960a](https://github.com/DIRACGrid/dirac-cwl/commit/240960acc94aeac5587efec28aa4431ae899225f))
* check if post-process succeeds ([f00a259](https://github.com/DIRACGrid/dirac-cwl/commit/f00a259c686ce6f0cb3f05b85cd8d78c8bb56990))
* execute the test only if the OS is Linux based ([98c39ab](https://github.com/DIRACGrid/dirac-cwl/commit/98c39ab06b0d1faf11197155af9e93b515781892))
* fix field condition in get_lfn ([31a6f62](https://github.com/DIRACGrid/dirac-cwl/commit/31a6f62f0b024794e4220685c23f7830e97471da))
* fix gaussian data generation test ([4a22720](https://github.com/DIRACGrid/dirac-cwl/commit/4a2272082a601f788cc1fb28b518c7c46f7dd54e))
* fix input paths for inputs in sandbox ([112bb23](https://github.com/DIRACGrid/dirac-cwl/commit/112bb23ff5776e4b795a10ec1c52576ba416c2be))
* fix lfn path for files of the same cwl output ([3ef7091](https://github.com/DIRACGrid/dirac-cwl/commit/3ef709165451c357b65d41d5cffc543cc596baa7))
* fix lint issues ([112980c](https://github.com/DIRACGrid/dirac-cwl/commit/112980ce4a2f1a1d859d2447aa73200736ec423a))
* fix output_se in hook instanciation ([d14ddd8](https://github.com/DIRACGrid/dirac-cwl/commit/d14ddd8e88107d640e34048c5435fa7ad61e59c7))
* fix parameter file being set twice ([3965240](https://github.com/DIRACGrid/dirac-cwl/commit/39652401797417c2c3e6d410440b22b97a68070a))
* fix sandbox mock ([49f4ce0](https://github.com/DIRACGrid/dirac-cwl/commit/49f4ce03cbfb28a2994ffe87807531593561294e))
* fix sandboxstoreClient used for sandbox downloads ([eef88c9](https://github.com/DIRACGrid/dirac-cwl/commit/eef88c934a4de2b8c6d4d9dbcfe01726cd2a3868))
* fix type error in lhcb post-process ([c94ea41](https://github.com/DIRACGrid/dirac-cwl/commit/c94ea41f56864800b765a887d2217348edfd7656))
* fixed merge conflicts ([f54aeee](https://github.com/DIRACGrid/dirac-cwl/commit/f54aeee2377121b4ee8a89270732d56da2369d48))
* fixed some tests ([836c8af](https://github.com/DIRACGrid/dirac-cwl/commit/836c8af02020fbf7c3176224f34f5156bcae685f))
* introduce a method to iteratively load the referenced subworkflows ([535ba34](https://github.com/DIRACGrid/dirac-cwl/commit/535ba341a1a4a40de9fedee1cb393ac8fd92a6d2))
* **job:** generate outputs in the worker node directory instead of at the root of the repo ([dde44e4](https://github.com/DIRACGrid/dirac-cwl/commit/dde44e464bba6dbeb592c62655db731a7f67209d))
* lfns start with a `/` and handle uppercase prefix ([712f880](https://github.com/DIRACGrid/dirac-cwl/commit/712f880268a24a1d01f43c58d3996563351475a5))
* load parameter file using cwltool in JobWrapper ([9c516a7](https://github.com/DIRACGrid/dirac-cwl/commit/9c516a73999ee182aed7444f43b6e75411665233))
* mandelbrot wf ([#4](https://github.com/DIRACGrid/dirac-cwl/issues/4)) ([732b757](https://github.com/DIRACGrid/dirac-cwl/commit/732b757fb2bd73184f51119c3404cdf80aa237e2))
* moved sandboxstore creation to create_sandbox (otherwise, it wasn't existing if we didn't call the function from the SubmissionClient) ([e841396](https://github.com/DIRACGrid/dirac-cwl/commit/e8413969f89195b12934177271e51943fc12d777))
* output sandbox is never uploaded ([82cd131](https://github.com/DIRACGrid/dirac-cwl/commit/82cd13114ba725ebdbcd3b8ead19466e655a0883))
* pass configuration parameters to jobs ([9d2b09d](https://github.com/DIRACGrid/dirac-cwl/commit/9d2b09d8f39efd34148a32832cf87374ef0f7421))
* pre-commit ([716e3f2](https://github.com/DIRACGrid/dirac-cwl/commit/716e3f22733cadd1165c968decf4e8050e8a6572))
* pre-commit ([94027ee](https://github.com/DIRACGrid/dirac-cwl/commit/94027eee98ac6ad687d8a15229c9f7c3da76fa5c))
* **production:** get default values even if input name is different from source name ([4920780](https://github.com/DIRACGrid/dirac-cwl/commit/49207800825de2b88f3d3ea94d53aa840b92afff))
* **QueryBasedPlugin:** Detect case where job_type is None ([81dff51](https://github.com/DIRACGrid/dirac-cwl/commit/81dff514f12239f696553c48f3888d57a095d691))
* Remove 'PrePostProcessingHint' from the 'extract_dirac_hints' docstring ([bb0cc6a](https://github.com/DIRACGrid/dirac-cwl/commit/bb0cc6af38bfa7864c9a4e4bba302b5527ee790d))
* Remove 'utils.py' file at dirac_cwl_proto/execution_hooks ([1b454c5](https://github.com/DIRACGrid/dirac-cwl/commit/1b454c5865fe2531f14fc1c69cc4fab7f264334e))
* remove duplicate code ([8b9e320](https://github.com/DIRACGrid/dirac-cwl/commit/8b9e320ec19a9a1b9d4ab8fecbb29e47d1eaad41))
* removed comments ([e5a11d1](https://github.com/DIRACGrid/dirac-cwl/commit/e5a11d1ae56b58d0c2c86b32caf73b68e47a647d))
* removed MockDiracXSandboxAPI and sandbox_store_client to match diracx ([830752b](https://github.com/DIRACGrid/dirac-cwl/commit/830752bb4d902b57a42578a6f6c793305072cba3))
* removed old MockSandboxStoreClient and forgotten pass ([d302395](https://github.com/DIRACGrid/dirac-cwl/commit/d302395549b80c5b1ccf9dad45a4e9b21e0ebabe))
* renamed upload_sandbox to create_sandbox to match diracx and imported the correct methods depending on LOCAL variable ([f4fcab0](https://github.com/DIRACGrid/dirac-cwl/commit/f4fcab0e808f15455f78399cca7d36959749b019))
* renaming ([91e04c6](https://github.com/DIRACGrid/dirac-cwl/commit/91e04c660a46c702e77c6ef75a50e51dc6339c2e))
* revert transformation changes and fix task default values ([dd3e8ff](https://github.com/DIRACGrid/dirac-cwl/commit/dd3e8ff2a20d9342775b3c14e714dcd7cd0e0758))
* test and mypy ([1d8949d](https://github.com/DIRACGrid/dirac-cwl/commit/1d8949d60065f394144a2e6bb23ece1814e77a0d))
* **tests:** Fix parallel test execution, again ([28e7167](https://github.com/DIRACGrid/dirac-cwl/commit/28e7167ef3a1fecdf1c4a69f9694ef60bcc36784))
* **tests:** Fix parallel test execution, again ([c0c0247](https://github.com/DIRACGrid/dirac-cwl/commit/c0c024743ebfd6a6286c140754a5469ded269892))
* **tests:** Fix time error margin at test_run_job_parallely ([108c451](https://github.com/DIRACGrid/dirac-cwl/commit/108c451b82743bf2ab16ddfd4b8cc528a1921970))
* **tests:** Fix time error margin at test_run_job_parallely ([c491eb8](https://github.com/DIRACGrid/dirac-cwl/commit/c491eb8175b329bf5c057fb74117355e2a00eb8e))
* typos ([40368f6](https://github.com/DIRACGrid/dirac-cwl/commit/40368f67b064d6b8c40a67785bbc63468577880f))
* update output workflows in tests ([a3517ce](https://github.com/DIRACGrid/dirac-cwl/commit/a3517ce2903be6cfaebde86f992b3dda8b8d7aa7))
* use job wrapper's return value in template ([3544e59](https://github.com/DIRACGrid/dirac-cwl/commit/3544e597e3ce8d03d82720bf0ce9d34bdc3b4de6))
* use job wrapper's return value in template ([ef945c6](https://github.com/DIRACGrid/dirac-cwl/commit/ef945c69a6611cdd452a1f8d26c2f2f298e696d2))
* use Pixi `workspace` instead of `project` ([931951c](https://github.com/DIRACGrid/dirac-cwl/commit/931951ca3a63e7afda6f38720f25df5afeef7c2a))
* use the cwl_utils.pack method to resolve references ([b98ddd2](https://github.com/DIRACGrid/dirac-cwl/commit/b98ddd2bdccdf23f297ba363fa15484d67eec825))
* various fixes and add logs info for outputs ([d4f6eb2](https://github.com/DIRACGrid/dirac-cwl/commit/d4f6eb21ab026dfdeb8a35d5f9df18960b862262))


### Performance Improvements

* use load_inputfile only on the cwl part ([1dea272](https://github.com/DIRACGrid/dirac-cwl/commit/1dea27213012cfc9a8643f3a0152f782d3990ae1))
