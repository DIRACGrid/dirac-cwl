# Design: Input Sandbox via Replica Map

## Context

The `dirac:Job` hint declares input sandbox files via `input_sandbox` source references:

```yaml
hints:
  - class: dirac:Job
    input_sandbox:
      - source: helper_script
      - source: config_files
        path: "conf/"
```

These reference CWL inputs whose values are files that need to be available on the worker node. The sandbox store handles upload/download of tar archives.

## Design

### Client Side (before submission)

1. User uploads sandbox files via the sandbox store API → receives a sandbox ID (e.g., `SB:SandboxSE|/S3/diracx-sandbox-store/sha256:abc123.tar.zst`)
2. User references the sandbox in their input YAML using a `SB:` prefixed path:

```yaml
# input.yaml
helper_script:
  class: File
  path: "SB:SandboxSE|/S3/diracx-sandbox-store/sha256:abc123.tar.zst:helper.sh"

config_files:
  - class: File
    path: "SB:SandboxSE|/S3/diracx-sandbox-store/sha256:abc123.tar.zst:config/app.yaml"
  - class: File
    path: "SB:SandboxSE|/S3/diracx-sandbox-store/sha256:abc123.tar.zst:config/db.yaml"
```

The format is `SB:<sandbox_id>:<relative_path_inside_tar>`.

### Submission (diracx-logic)

No changes needed. `submit_cwl_jobs` stores the input YAML as-is in `workflow_params`. The `SB:` paths are opaque strings at this stage.

### Worker Side (JobWrapper)

Changes in `JobWrapper.__download_input_sandbox`:

1. Iterate `input_sandbox` sources from the hint
2. For each source, extract the CWL input value from `workflow_params`
3. Detect `SB:` prefixed paths
4. Parse the sandbox ID and relative path: `SB:<sandbox_id>:<relative_path>`
5. Download the sandbox tar (once per unique sandbox ID — cache across sources)
6. Extract to the job directory, respecting the hint's `path` field for subdirectory placement
7. Add a replica map entry mapping the `SB:` path to the local extracted file path

```python
# Pseudo-code for the worker-side resolution
for ref in job_hint.input_sandbox:
    cwl_value = inputs.cwl.get(ref.source)
    for file_path in extract_file_paths(cwl_value):
        if file_path.startswith("SB:"):
            sandbox_id, relative_path = parse_sb_path(file_path)

            # Download + extract sandbox (cached per sandbox_id)
            extract_dir = download_and_extract_sandbox(sandbox_id, job_path)

            # Determine local path (respecting hint's path field)
            dest_dir = job_path / ref.path if ref.path else job_path
            local_path = dest_dir / relative_path

            # Add to replica map for the executor
            replica_map[file_path] = local_path
```

After this, the `DiracReplicaMapFsAccess` resolves `SB:` paths exactly like `LFN:` paths — the executor doesn't need to know the difference.

### Replica Map as Universal Resolution Layer

With this design, the replica map resolves all file types uniformly:

| Prefix | Source | Resolution |
|--------|--------|------------|
| `LFN:` | Grid storage | `DataManager.getActiveReplicas()` → PFN URLs |
| `SB:` | Sandbox store | Download tar → extract → local file path |
| *(none)* | Local file | Direct path (already on worker) |

The CWL executor sees only local paths or URLs via the replica map — it never handles sandbox or grid storage directly.

### Changes Required

| Location | Change |
|----------|--------|
| `JobWrapper.__download_input_sandbox` | Detect `SB:` paths, download/extract tar, add to replica map |
| `DiracReplicaMapFsAccess._resolve_lfn` | Handle `SB:` prefix in addition to `LFN:` (or rename to `_resolve_path`) |
| `JobWrapper.__build_replica_map` | Accept sandbox entries in addition to LFN entries |

### Sandbox Caching

Multiple `input_sandbox` sources may reference the same sandbox tar (same `sandbox_id`, different `relative_path`). The download and extraction should happen once per unique sandbox ID:

```python
extracted_sandboxes: dict[str, Path] = {}  # sandbox_id → extracted directory

for sandbox_id in unique_sandbox_ids:
    if sandbox_id not in extracted_sandboxes:
        extracted_sandboxes[sandbox_id] = download_and_extract(sandbox_id, job_path)
```

### Parametric Jobs

For parametric jobs (same CWL, different inputs), sandboxes may be shared:
- Same sandbox tar for all jobs → uploaded once by client, same `SB:` ID in all input YAMLs
- Different sandbox per job → different tars, different `SB:` IDs

The `workflows` table deduplication handles the CWL. The sandbox store handles file deduplication. `workflow_params` carries the per-job `SB:` references.

### Open Questions

1. **Sandbox path format** — is `SB:<sandbox_id>:<relative_path>` the right format, or should we use a different separator? The sandbox ID itself may contain colons.
2. **Tar extraction** — does `download_sandbox` already extract, or do we need to handle `zstd` decompression + tar extraction ourselves?
3. **Sandbox assignment** — does the sandbox need to be assigned to the job via `assign_sandbox_to_job` before the worker can download it?
