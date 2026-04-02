**Status: Draft for Discussion**

## Context: The Three-Level CWL Model

DIRAC CWL workflows operate at three levels, determined by **which hints are present** (not by CWL class type):

| Level | DIRAC Concept | CWL Hint | Determines |
|-------|---------------|----------|------------|
| **Production** | Production/Request | `dirac:Production` | Input dataset sourcing, orchestrates transformations |
| **Transformation** | Transformation | `dirac:Transformation` | Job template — grouping, input queries |
| **Job** | Job | `dirac:Job` *(new, replaces `dirac:Scheduling` + `dirac:ExecutionHooks`)* | Single execution: scheduling, I/O, hooks |

A CWL Workflow with `dirac:Production` creates Transformations from its steps. Each Transformation is a job template that creates many Jobs. The `dirac:Job` hint lives at the **Job level** — it tells DIRAC how to schedule and manage a single execution.

This design focuses on the **Job level**: the `dirac:Job` hint and the diracX submission endpoint.

## Problem Statement

At the moment in DIRAC, CWL jobs are submitted to DIRAC by:
1. Setting `Executable = "dirac-cwl-exec"` in the JDL
2. Shipping the CWL definition (`job.json`) as an InputSandbox file
3. At runtime on the worker node, `__createCWLJobWrapper` clones the `dirac-cwl` repo, installs pixi, downloads the sandbox, and runs the CWL

This has several problems:
- The CWL workflow definition is opaque to DIRAC — it's just a sandbox blob, not queryable or inspectable
- `dirac:Scheduling` and `dirac:ExecutionHooks` are separate hints that duplicate concepts already present in JDL (`Site`, `Priority`, `OutputSandbox`, etc.)
- The existing `convert_to_jdl()` in `submission_clients.py` only maps ~40% of available JDL fields
- There is no native diracX API for CWL submission — everything goes through the JDL path
- The worker-node shim `git clone`s dirac-cwl and installs pixi on every job

## Goals

1. **Unified `dirac:Job` hint** — replace `dirac:Scheduling` and `dirac:ExecutionHooks` with a single versioned hint
2. **Dedicated `workflows` table** — store CWL definitions once, content-addressed by SHA-256; jobs reference a workflow, not embed it
3. **`job_workflow_params` table** — per-job parameters stored separately, lightweight; 10k parametric jobs = 1 workflow row + 10k param rows
4. **New diracX endpoint** — `POST /api/jobs/` accepts CWL + input YAML(s) directly
5. **Models in diracX** — `JobSubmissionModel`, `JobHint`, and related types live in diracX (migrated from dirac-cwl)
6. **No git clone on worker nodes** — dirac-cwl is installed in the diracX environment; job wrapper is accessed via `importlib.resources`
7. **Fail fast** — strict validation of all CWL ID references, types, and hint fields at submission time

## Design

### 1. The `dirac:Job` Hint

#### Design principles

1. **Use standard CWL where possible** — don't duplicate what CWL already provides natively via `requirements`
2. **Execution hooks are not user-configured** — they are determined automatically by `type`; the submitter doesn't choose them
3. **Derive what you can** — `job_name` from CWL `label`/`id`, processors from `ResourceRequirement`, etc.
4. **Reference CWL I/O by source ID** — instead of duplicating file lists, use `source:` to point to CWL input/output IDs
5. **Versioned schema** — the hint carries a `schema_version` to enable forward-compatible evolution

#### What CWL already provides (via `requirements`)

These standard CWL constructs map directly to JDL fields without needing a `dirac:Job` field:

| CWL Requirement | CWL Field | JDL Equivalent | Notes |
|-----------------|-----------|----------------|-------|
| `ResourceRequirement` | `coresMin` | `MinNumberOfProcessors` | |
| `ResourceRequirement` | `coresMax` | `MaxNumberOfProcessors` | |
| `ResourceRequirement` | `ramMin` | `MinRAM` | |
| `ResourceRequirement` | `ramMax` | `MaxRAM` | |
| `ToolTimeLimit` | `timelimit` | — | Wall-clock seconds; see [CPUTime](#cputime-and-cpu-work) |
| `DockerRequirement` | `dockerPull` | — | Container support TBD (unrelated to `Platform`) |
| `CUDARequirement` | *(presence)* | `Tags: ["GPU"]` | Implies GPU tag |
| `MPIRequirement` | *(presence)* | — | **Not supported** — raises `NotImplementedError` |
| *(CWL task)* | `label` or `id` | `JobName` | Derived automatically |

#### CPUTime and CPU work

DIRAC's `CPUTime` is **normalized CPU work** in HS06-seconds (`wall_time * CPUNormalizationFactor`), not wall-clock time. CWL's `ToolTimeLimit` is wall-clock seconds. These are fundamentally different units.

**Approach**: The `dirac:Job` hint provides an explicit `cpu_work` field representing normalized HS06-seconds — the same unit DIRAC uses internally. This avoids ambiguity:

- `cpu_work` in `dirac:Job` → maps directly to JDL `CPUTime` (HS06-seconds)
- `ToolTimeLimit` (if present) → used by cwltool for local execution; **not** translated to `CPUTime`

The normalization factor itself can be calculated by DB12 on the worker node. Users who think in wall-clock terms can compute `cpu_work = wall_seconds * estimated_HS06_factor`.

#### What `dirac:Job` adds (DIRAC-specific, no CWL equivalent)

```yaml
cwlVersion: v1.2
class: CommandLineTool

requirements:
  - class: ResourceRequirement
    coresMin: 1
    coresMax: 4
    ramMin: 2048        # MB

hints:
  - class: dirac:Job
    schema_version: "1.0"

    # --- Scheduling ---
    priority: 5
    cpu_work: 864000              # HS06-seconds (= CPUTime in JDL)
    platform: "x86_64-el9"
    sites:
      - LCG.CERN.cern
      - LCG.IN2P3.fr
    banned_sites:
      - LCG.RAL.uk
    tags: ["GPU"]                 # additional tags beyond auto-derived ones

    # --- Job metadata ---
    type: "User"                  # determines execution hooks automatically
    group: "lhcb_analysis"
    log_level: "INFO"

    # --- I/O: reference CWL inputs/outputs by source ID ---
    input_sandbox:
      - source: helper_script               # CWL input ID (type: File) → job root
      - source: config_files                # CWL input ID (type: File[])
        path: "conf/"                       # relative to job working directory
    input_data:
      - source: input_lfns                  # CWL input ID (type: File[])
    output_sandbox:
      - source: stderr_log                  # CWL output ID
    output_data:
      - source: result_file                 # CWL output ID
        output_path: "/lhcb/user/r/roneil/output/"
        output_se: ["SE-USER"]
      - source: histogram                   # CWL output ID
        output_path: "/lhcb/user/r/roneil/histos/"
        output_se: ["SE-AUXILIARY"]

label: "my-analysis-job"   # → becomes JobName

inputs:
  - id: helper_script
    type: File
  - id: config_files
    type: File[]
  - id: input_lfns
    type: File[]
  - id: config_param
    type: string

outputs:
  - id: result_file
    type: File
    outputBinding:
      glob: "result.root"
  - id: histogram
    type: File
    outputBinding:
      glob: "histos.root"
  - id: stderr_log
    type: File
    outputBinding:
      glob: "std.err"

$namespaces:
  dirac: "schemas/dirac-metadata.json#/$defs/"
```

Note: no `baseCommand` — the executor is always the dirac-cwl runner. The CWL task defines **what** to run; DIRAC handles **how** to run it.

#### Key design decisions

**`type` instead of `job_type`**: Shorter, cleaner, and consistent with CWL's own `class:` convention. Maps to JDL `JobType`. Determines execution hooks automatically (see [Execution hooks](#execution-hooks-automatic-not-user-configured)).

**Sites: `sites` + `banned_sites`** as flat lists:

```yaml
# Run only at these sites:
sites:
  - LCG.CERN.cern
  - LCG.IN2P3.fr

# Exclude specific sites:
banned_sites:
  - LCG.RAL.uk
```

DIRAC computes the effective set as `Sites - BannedSites`. If `sites` is omitted or empty, the job can run anywhere (equivalent to `Site = ANY`). Both fields are optional flat lists — simple to read and write, no nesting required. The semantics mirror DIRAC's native model directly.

**I/O by CWL source ID** using CWL-idiomatic `source:` syntax:

Each I/O entry uses `source:` to reference a CWL input or output by its `id`, mirroring CWL's own `outputSource` convention:

- `input_sandbox: [{source: helper_script}]` — the CWL input with `id: helper_script` (must be `type: File` or `File[]`) will be uploaded to the DIRAC sandbox store and delivered to the worker node. An optional `path:` specifies a relative directory within the job working directory (e.g., `path: "conf/"` places the file(s) in `<job_root>/conf/`). If omitted, files land in the job root
- `input_data: [{source: input_lfns}]` — the CWL input with `id: input_lfns` will be resolved as LFN paths and registered as `InputData` in the JDL for data-driven scheduling
- `output_sandbox: [{source: stderr_log}]` — the CWL output with `id: stderr_log` will be uploaded to the sandbox store after execution
- `output_data: [{source: result_file, output_path: "/lhcb/...", output_se: ["SE-USER"]}]` — the CWL output with `id: result_file` will be registered in the file catalog at the given LFN path, on the specified storage element(s)

The `source:` syntax is:
- **CWL-idiomatic** — consistent with how CWL references inputs/outputs elsewhere
- **Extensible** — per-entry metadata (like `output_se`, `output_path`, `path`) lives alongside the source reference
- **Per-output SE** — each `output_data` entry specifies its own `output_se`, allowing different outputs to go to different storage elements (e.g., large data to tape, small histograms to disk)

All referenced IDs are **strictly validated** at submission time — the translation layer verifies that each `source` ID exists in the CWL task's inputs/outputs and has a compatible type (`File` or `File[]`). Invalid references fail the submission immediately.

**Schema versioning**: Every `dirac:Job` hint must carry a `schema_version` field. This enables the system to:
- Reject hints with unsupported versions
- Evolve the schema without breaking existing workflows
- Provide clear error messages when a workflow targets a newer schema version

#### Field mapping summary

| Source | Field | JDL Equivalent | In `dirac:Job`? |
|--------|-------|----------------|-----------------|
| CWL `label`/`id` | *(auto)* | `JobName` | No — derived |
| `ResourceRequirement.coresMin` | *(auto)* | `MinNumberOfProcessors` | No — CWL native |
| `ResourceRequirement.coresMax` | *(auto)* | `MaxNumberOfProcessors` | No — CWL native |
| `ResourceRequirement.ramMin` | *(auto)* | `MinRAM` | No — CWL native |
| `ResourceRequirement.ramMax` | *(auto)* | `MaxRAM` | No — CWL native |
| `CUDARequirement` | *(auto)* | `Tags += ["GPU"]` | No — CWL native |
| `MPIRequirement` | — | — | **NotImplementedError** |
| `dirac:Job` | `schema_version` | — | **Yes** — required |
| `dirac:Job` | `cpu_work` | `CPUTime` | **Yes** — HS06-seconds |
| `dirac:Job` | `priority` | `Priority` | **Yes** |
| `dirac:Job` | `platform` | `Platform` | **Yes** |
| `dirac:Job` | `sites` | `Site` | **Yes** |
| `dirac:Job` | `banned_sites` | `BannedSites` | **Yes** |
| `dirac:Job` | `tags` | `Tags` (merged with auto) | **Yes** |
| `dirac:Job` | `type` | `JobType` | **Yes** |
| `dirac:Job` | `group` | `JobGroup` | **Yes** |
| `dirac:Job` | `log_level` | `LogLevel` | **Yes** |
| `dirac:Job` | `input_sandbox[].source` | `InputSandbox` | **Yes** — CWL input IDs |
| `dirac:Job` | `input_sandbox[].path` | *(worker-side)* | **Yes** — relative directory |
| `dirac:Job` | `input_data[].source` | `InputData` | **Yes** — CWL input IDs |
| `dirac:Job` | `output_sandbox[].source` | `OutputSandbox` | **Yes** — CWL output IDs |
| `dirac:Job` | `output_data[].source` | `OutputData` | **Yes** — CWL output ID |
| `dirac:Job` | `output_data[].output_path` | `OutputPath` | **Yes** — per-output LFN path |
| `dirac:Job` | `output_data[].output_se` | `OutputSE` | **Yes** — per-output SE list |
| *(system)* | `Executable` | `dirac-cwl-exec` | No — always set |
| *(system)* | `Owner`, `OwnerGroup`, `VO` | *(from auth)* | No — injected |
| *(system)* | `Status`, `MinorStatus` | *(managed)* | No |
| *(system)* | `JobID` | *(auto)* | No |

#### Execution hooks: automatic, not user-configured

Execution hooks are **derived from `type`**:

- `type: "User"` → `QueryBasedPlugin` (default)
- `type: "MCSimulation"` → VO-specific simulation plugin
- etc.

The hook plugin registry (currently in dirac-cwl, eventually migrated to diracX as entrypoints) handles discovery by VO and type. The `dirac:Job` hint does **not** expose `hook_plugin` or `hook_config` — these are internal to the system.

`output_data` (with per-output `output_se` and `output_path`) and `output_sandbox` remain in `dirac:Job` because they are user-specified data management choices, not hook configuration.

### 2. Storage Model: `workflows` + `job_workflow_params`

Instead of embedding CWL into each JDL row (which would duplicate the CWL blob across thousands of parametric jobs), CWL definitions are stored **once** in a dedicated table, and jobs reference them.

#### Schema

```sql
CREATE TABLE workflows (
    workflow_id  CHAR(64) PRIMARY KEY,  -- SHA-256 of the CWL content
    cwl          MEDIUMTEXT NOT NULL,    -- CWL YAML (original, uncompressed)
    persistent   BOOL NOT NULL DEFAULT FALSE,
    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- New columns on the existing Jobs table:
ALTER TABLE Jobs
    ADD COLUMN workflow_id      CHAR(64) DEFAULT NULL,     -- FK → workflows.workflow_id
    ADD COLUMN workflow_params  JSON DEFAULT NULL,          -- immutable per-job input parameters
    ADD FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id);
```

`workflow_params` is an **immutable** JSON column — once set at job creation, it is never updated. It holds the per-job CWL input parameters (the content of an input YAML). Non-CWL jobs leave both columns `NULL`.

#### How it works

1. **Workflow insertion** — on submission, the CWL content is SHA-256 hashed. If the hash already exists in `workflows`, the insert is skipped (content-addressed, immutable). Workflows are never edited — a changed CWL produces a new hash.

2. **Job creation** — each job row in `Jobs` gets a `workflow_id` reference and its own `workflow_params` JSON. This is where per-job variation lives — co-located with the job, no extra join needed.

3. **Parametric jobs** — submitting 10k jobs with the same CWL but different parameters produces:
   - 1 row in `workflows` (insert-if-not-exists)
   - 10k rows in `Jobs` with the same `workflow_id` but different `workflow_params` (lightweight JSON)

4. **`persistent` flag** — controls cleanup behavior:
   - `persistent = FALSE` (default): ad-hoc user jobs; workflow row can be cleaned up when no jobs reference it
   - `persistent = TRUE`: production/transformation workflows; retained indefinitely

5. **Worker-side retrieval** — the job wrapper fetches the CWL via diracX API using the `workflow_id` from its `Jobs` row, and reads input parameters from `workflow_params`. No sandbox involved for the workflow definition.

#### Parameter mapping via `dirac:Job` hint

The `dirac:Job` hint tells DIRAC which CWL inputs should be promoted to job-level parameters visible to the scheduler:

```yaml
hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User
    # ... scheduling fields ...

    # Which CWL inputs become job-visible parameters
    input_data:
      - source: input_lfns      # CWL input ID → resolved to InputData for scheduling
    input_sandbox:
      - source: helper_script   # CWL input ID → files uploaded to sandbox
```

At submission time, the translation layer reads these mappings to populate JDL fields (for the transition period) or job attributes (post-JDL) from the per-job parameters.

#### Why not CWL-in-JDL?

| Concern | CWL-in-JDL (previous) | `workflows` table (current) |
|---------|----------------------|----------------------------|
| 10k parametric jobs | 10k copies of compressed CWL | 1 workflow row + 10k param rows |
| Storage | ~16MB CWL blob per JDL row | CWL stored once |
| Queryability | Opaque base64 blob | CWL stored as readable YAML |
| Immutability | Mutable (JDL can be updated) | Content-addressed, immutable |
| Cleanup | Tied to JDL lifecycle | Independent lifecycle via `persistent` flag |

### 3. New diracX Endpoint: `POST /api/jobs/`

#### Request format

The endpoint accepts a **CWL workflow file** plus one or more **input YAML files**. Each input YAML produces a separate job:

```
POST /api/jobs/
Content-Type: multipart/form-data

workflow: <wf.cwl>           # CWL workflow/tool definition (YAML)
inputs[]: <input1.yaml>      # Input parameters for job 1
inputs[]: <input2.yaml>      # Input parameters for job 2
```

This produces 2 jobs:
- Job 1: run `wf.cwl` with `input1.yaml`
- Job 2: run `wf.cwl` with `input2.yaml`

If no input files are provided, a single job is created with no inputs (suitable for tools with no required inputs or all defaults).

#### Translation flow

```
POST /api/jobs/
  │
  ▼
Router (diracx-routers)
  │  Parses multipart: CWL YAML + input YAML(s)
  │  Validates CWL via JobSubmissionModel (pydantic)
  │  Validates schema_version
  ▼
Logic (diracx-logic)
  │  SHA-256 hash CWL → INSERT INTO workflows IF NOT EXISTS
  │  For each input YAML:
  │    Extracts dirac:Job hint from CWL task
  │    Extracts ResourceRequirement, CUDARequirement, etc.
  │    Derives JobName from CWL label/id
  │    Resolves I/O: source IDs → file paths/LFNs (strict validation)
  │    Maps all → JDL fields (transition period)
  │    Calls existing submit_jdl_jobs() with generated JDL
  │    Sets workflow_id + workflow_params (JSON) on Jobs row
  ▼
DB
  │  CWL stored once in workflows table
  │  JDL stored in JobJDLs (transition period)
  │  Job attrs + workflow_id + workflow_params in Jobs table
  ▼
Returns list[InsertedJob]
```

During the transition period, JDL is still generated for compatibility with existing DIRAC infrastructure (matcher, optimizer, etc.). The `workflows` table + `workflow_params` column are the source of truth for the CWL definition and per-job parameters. Once JDL is fully retired, the JDL generation step is removed.

#### Translation logic (new functions in diracx-logic)

```python
import hashlib
import json

from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import (
    ResourceRequirement, CUDARequirement, MPIRequirement,
)


SUPPORTED_SCHEMA_VERSIONS = {"1.0"}


def compute_workflow_id(cwl_yaml: str) -> str:
    """Content-address a CWL workflow by its SHA-256 hash."""
    return hashlib.sha256(cwl_yaml.encode()).hexdigest()


async def submit_cwl_jobs(
    cwl_yaml: str,
    input_yamls: list[str],
    db: JobDB,
) -> list[InsertedJob]:
    """Submit CWL jobs: store workflow once, create one job per input YAML."""
    workflow_id = compute_workflow_id(cwl_yaml)

    # INSERT IF NOT EXISTS — idempotent, content-addressed
    await db.insert_workflow(workflow_id, cwl_yaml, persistent=False)

    task = parse_cwl(cwl_yaml)
    job_hint = JobHint.from_cwl(task)

    if job_hint.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise ValueError(
            f"Unsupported dirac:Job schema_version '{job_hint.schema_version}'. "
            f"Supported: {SUPPORTED_SCHEMA_VERSIONS}"
        )

    inserted = []
    for input_yaml in input_yamls:
        inputs = parse_inputs(input_yaml) if input_yaml else None
        workflow_params = json.loads(input_yaml) if input_yaml else None

        # Generate JDL for transition period
        jdl = cwl_to_jdl(task, job_hint, inputs)

        # Submit via existing pipeline
        jobs = await submit_jdl_jobs([jdl])

        # Set workflow reference + immutable params on job row
        for job in jobs:
            await db.set_workflow_ref(
                job.job_id,
                workflow_id=workflow_id,
                workflow_params=workflow_params,
            )
        inserted.extend(jobs)

    return inserted


def cwl_to_jdl(
    task: CommandLineTool | Workflow | ExpressionTool,
    job_hint: JobHint,
    inputs: JobInputModel | None,
) -> str:
    """Convert a CWL task with dirac:Job hint into a JDL string.

    This is a transition-period function — once JDL is retired,
    job attributes are populated directly from the hint + CWL.
    """
    jdl_fields = {
        "Executable": "dirac-cwl-exec",
        "JobType": job_hint.type,
        "Priority": job_hint.priority,
        "LogLevel": job_hint.log_level,
    }

    if job_hint.cpu_work:
        jdl_fields["CPUTime"] = job_hint.cpu_work
    if job_hint.platform:
        jdl_fields["Platform"] = job_hint.platform

    # Derive JobName from CWL label/id
    task_label = getattr(task, "label", None)
    task_id = getattr(task, "id", None)
    if task_label:
        jdl_fields["JobName"] = task_label
    elif task_id and task_id != ".":
        jdl_fields["JobName"] = task_id.split("#")[-1].split("/")[-1]

    # Extract from CWL requirements (standard CWL, not dirac:Job)
    tags = set(job_hint.tags or [])
    for req in (getattr(task, "requirements", None) or []):
        if isinstance(req, ResourceRequirement):
            if req.coresMin:
                jdl_fields["MinNumberOfProcessors"] = int(req.coresMin)
            if req.coresMax:
                jdl_fields["MaxNumberOfProcessors"] = int(req.coresMax)
            if req.ramMin:
                jdl_fields["MinRAM"] = int(req.ramMin)
            if req.ramMax:
                jdl_fields["MaxRAM"] = int(req.ramMax)
        elif isinstance(req, CUDARequirement):
            tags.add("GPU")
        elif isinstance(req, MPIRequirement):
            raise NotImplementedError(
                "MPIRequirement is not yet supported for DIRAC CWL jobs"
            )

    # Auto-derive processor tags
    min_proc = jdl_fields.get("MinNumberOfProcessors", 1)
    max_proc = jdl_fields.get("MaxNumberOfProcessors")
    if min_proc and min_proc > 1:
        tags.add("MultiProcessor")
    if min_proc and max_proc and min_proc == max_proc:
        tags.add(f"{min_proc}Processors")

    if tags:
        jdl_fields["Tags"] = list(tags)

    # Sites
    if job_hint.sites:
        jdl_fields["Site"] = job_hint.sites
    if job_hint.banned_sites:
        jdl_fields["BannedSites"] = job_hint.banned_sites

    if job_hint.group:
        jdl_fields["JobGroup"] = job_hint.group

    # Resolve I/O from CWL input/output source IDs
    cwl_input_ids = {_extract_id(inp.id): inp for inp in (task.inputs or [])}
    cwl_output_ids = {_extract_id(out.id): out for out in (task.outputs or [])}

    # InputSandbox
    if job_hint.input_sandbox:
        sandbox_files = []
        for ref in job_hint.input_sandbox:
            _validate_cwl_id(ref.source, cwl_input_ids, "input", ["File", "File[]"])
            if inputs and ref.source in inputs.cwl:
                sandbox_files.extend(_extract_file_paths(inputs.cwl[ref.source]))
        if sandbox_files:
            jdl_fields["InputSandbox"] = sandbox_files

    # InputData
    if job_hint.input_data:
        lfns = []
        for ref in job_hint.input_data:
            _validate_cwl_id(ref.source, cwl_input_ids, "input", ["File", "File[]"])
            if inputs and ref.source in inputs.cwl:
                lfns.extend(_extract_lfn_paths(inputs.cwl[ref.source]))
        if lfns:
            jdl_fields["InputData"] = lfns

    # OutputSandbox
    if job_hint.output_sandbox:
        sandbox_outputs = []
        for ref in job_hint.output_sandbox:
            _validate_cwl_id(ref.source, cwl_output_ids, "output", ["File", "File[]"])
            out = cwl_output_ids[ref.source]
            if hasattr(out, "outputBinding") and out.outputBinding:
                sandbox_outputs.append(out.outputBinding.glob)
        if sandbox_outputs:
            jdl_fields["OutputSandbox"] = sandbox_outputs

    # OutputData (per-output SE and path)
    if job_hint.output_data:
        output_files = []
        all_ses = set()
        for entry in job_hint.output_data:
            _validate_cwl_id(entry.source, cwl_output_ids, "output", ["File", "File[]"])
            out = cwl_output_ids[entry.source]
            if hasattr(out, "outputBinding") and out.outputBinding:
                output_files.append(out.outputBinding.glob)
            all_ses.update(entry.output_se)
        if output_files:
            jdl_fields["OutputData"] = output_files
            jdl_fields["OutputPath"] = job_hint.output_data[0].output_path
            jdl_fields["OutputSE"] = list(all_ses)

    return format_as_jdl(jdl_fields)


def _extract_id(cwl_id: str) -> str:
    """Extract short ID from CWL full URI (e.g., 'file.cwl#input1' → 'input1')."""
    return cwl_id.split("#")[-1].split("/")[-1]
```

### 4. Changes to DIRAC Worker-Side Execution

#### Current flow (`__createCWLJobWrapper`):
```
git clone dirac-cwl → install pixi → download sandbox (gets job.json) → run wrapper
```

#### New flow:
```
Fetch CWL from workflows table (via diracX API) → read workflow_params from Jobs row → write job.json → run job wrapper (via importlib.resources)
```

Since dirac-cwl is installed as a package in the diracX environment, the job wrapper template is accessed via `importlib.resources` — no git clone or pixi install needed.

Changes in `__createCWLJobWrapper` in `Utils.py`:

1. Accept `jobParams` as a parameter (already available in `createJobWrapper`)
2. Fetch CWL definition from diracX API using `workflow_id` from the job
3. Read `workflow_params` (per-job input parameters) from the job
4. Write CWL + params to local files (`task.cwl`, `params.json`)
5. Remove the git clone, pixi install, and `dirac-wms-job-get-input` steps
6. Load the job wrapper template via `importlib.resources.files("dirac_cwl.job")`
7. InputSandbox is still used for actual input **files** — just not for the workflow definition

### 5. Pydantic Models (in diracX)

These models live in diracX (migrated from dirac-cwl). The old `SchedulingHint` and `ExecutionHooksHint` classes are removed — there is no backward compatibility layer.

```python
class IOSource(BaseModel):
    """Reference to a CWL input or output by its ID."""
    source: str             # CWL input/output ID
    path: str | None = None # relative path within job working directory (input_sandbox only)


class OutputDataEntry(BaseModel):
    """Output data entry with per-output SE and LFN path."""
    source: str                    # CWL output ID
    output_path: str               # LFN destination path
    output_se: list[str] = ["SE-USER"]


class JobHint(BaseModel, Hint):
    """Unified DIRAC-specific hint for job scheduling and I/O.

    Resource requirements (cores, RAM) are expressed via standard CWL
    requirements, not in this hint.

    Execution hooks are determined automatically by `type`, not
    configured by the submitter.

    I/O fields reference CWL input/output IDs via `source:` syntax,
    consistent with CWL's own referencing conventions.
    """
    schema_version: str  # required, e.g. "1.0"

    # Scheduling (DIRAC-specific, no CWL equivalent)
    priority: int = 5
    cpu_work: int | None = None   # HS06-seconds → JDL CPUTime
    platform: str | None = None
    sites: list[str] | None = None
    banned_sites: list[str] | None = None
    tags: list[str] | None = None  # merged with auto-derived tags

    # Job metadata
    type: str = "User"
    group: str = ""
    log_level: str = "INFO"

    # I/O: reference CWL input/output IDs via source:
    input_sandbox: list[IOSource] = []
    input_data: list[IOSource] = []
    output_sandbox: list[IOSource] = []
    output_data: list[OutputDataEntry] = []

    @classmethod
    def from_cwl(cls, cwl_object) -> "JobHint":
        hints = getattr(cwl_object, "hints", []) or []
        for hint in hints:
            if hint.get("class") == "dirac:Job":
                data = {k: v for k, v in hint.items() if k != "class"}
                return cls(**data)
        raise ValueError("CWL task is missing required dirac:Job hint")
```

### 6. Summary of Changes by Repository

#### diracx (primary)
| Change | Location |
|--------|----------|
| `JobHint`, `IOSource`, `OutputDataEntry` models | `diracx-core/src/diracx/core/models/` |
| `JobSubmissionModel`, `JobInputModel` models | `diracx-core/src/diracx/core/models/` |
| `workflows` table schema | `diracx-db/src/diracx/db/sql/job/schema.py` |
| `workflow_id` + `workflow_params` columns on `Jobs` table | `diracx-db/src/diracx/db/sql/job/schema.py` |
| New router `POST /api/jobs/` (multipart CWL + inputs) | `diracx-routers/src/diracx/routers/jobs/submission.py` |
| `GET /api/workflows/{workflow_id}` endpoint | `diracx-routers/src/diracx/routers/jobs/` |
| `submit_cwl_jobs()` + `cwl_to_jdl()` logic | `diracx-logic/src/diracx/logic/jobs/submission.py` |
| Job wrapper template (migrated from dirac-cwl) | `diracx-logic/src/diracx/logic/jobs/` |
| `dirac-cwl` as dependency | `pyproject.toml` |

#### dirac-cwl
| Change | Location |
|--------|----------|
| Remove `SchedulingHint` + `ExecutionHooksHint` | `execution_hooks/core.py` |
| Remove `convert_to_jdl()` | `job/submission_clients.py` |
| Update `JobWrapper.run_job()` to resolve hooks from `type` | `job/job_wrapper.py` |
| Update schema with `Job` definition (versioned) | `schemas/dirac-metadata.json` |
| Models migrated to diracX — remove from dirac-cwl | `submission_models.py` |

#### DIRAC
| Change | Location |
|--------|----------|
| Modify `__createCWLJobWrapper` to fetch CWL via diracX API + read `workflow_params` | `WorkloadManagementSystem/Utilities/Utils.py` |
| Remove git clone + pixi install from bash wrapper | Same file |
| Load job wrapper via `importlib.resources` | Same file |

### 7. Migration Path

**Phase 1** (this work):
- Implement `workflows` table and `workflow_id`/`workflow_params` columns on `Jobs`
- Implement `dirac:Job` hint and models in diracX
- Implement `POST /api/jobs/` endpoint + `GET /api/workflows/{workflow_id}`
- Implement `cwl_to_jdl()` transition shim
- Modify DIRAC `__createCWLJobWrapper` — remove git clone, use importlib.resources, fetch CWL from API
- Remove old hints from dirac-cwl

**Phase 2** (future, per production-plugin-system.md):
- `dirac:Transformation` hint → transformation submission endpoint
- `dirac:Production` hint → production orchestration endpoint
- Migrate execution hooks plugin registry to diracX entrypoints

### 8. Decisions Made

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | Models live in diracX, not dirac-cwl | Natural migration path; dirac-cwl components follow |
| D2 | Endpoint accepts raw CWL YAML + input YAMLs | User-friendly; `wf.cwl + input1.yaml + input2.yaml` → N jobs |
| D3 | Endpoint is `POST /api/jobs/` (not `/api/jobs/cwl`) | CWL becomes the primary submission format |
| D4 | `Platform` is CPU architecture (unrelated to containers) | Container support is a separate discussion |
| D5 | No git clone on worker nodes | dirac-cwl installed in diracX; wrapper via `importlib.resources` |
| D6 | Hook registry starts in dirac-cwl, migrates to diracX entrypoints | Incremental migration |
| D7 | `cpu_work` (HS06-seconds) in hint; `ToolTimeLimit` for local cwltool only | Avoids unit ambiguity; DB12 calculates normalization factor |
| D8 | Strict I/O validation — fail fast at submission | Prevents wasting CPU on 10^3-10^5 jobs with bad references |
| D9 | No backward compatibility with old hints | Nothing in production; simplifies implementation |
| D10 | Hints carry `schema_version` | Forward-compatible evolution |
| D11 | CWL stored in `workflows` table, not embedded in JDL | Content-addressed (SHA-256), immutable; 10k parametric jobs share 1 workflow row |
| D12 | Per-job params as immutable JSON column on `Jobs` table | Co-located with job, no extra join; immutable once set |
| D13 | JDL generation is a transition-period shim, not the target | `workflows` + `workflow_params` are source of truth; JDL generated for existing DIRAC infra compatibility |

### 9. Open Questions

1. **`ToolTimeLimit` in cwl_utils**: Need to verify cwl_utils parses it. If available, it can be used for local execution wall-clock limits alongside `cpu_work` for DIRAC scheduling. To be investigated.

2. **Container support**: How to run containerized jobs within DIRAC. Unrelated to `Platform` (which is CPU architecture). Separate design needed.

3. **Multipart API design**: Exact multipart field naming and how to handle optional inputs (no input YAML = single job with defaults). Also: should the endpoint support JSON as an alternative to YAML?

4. **Input YAML templating**: The endpoint naturally supports `wf.cwl + N input YAMLs → N jobs`. Future extension could support templating (e.g., parameter sweeps) — to be designed separately.

5. **Workflow cleanup policy**: When `persistent = FALSE`, what triggers cleanup of orphaned workflow rows? Options: periodic GC that checks for referencing jobs, TTL-based expiry, or tied to existing job cleanup routines.
