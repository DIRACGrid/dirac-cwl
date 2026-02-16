# Production Plugin System Design

## Overview

This document describes the design for a pluggable Production configuration system that makes the CWL executor (`__main__.py`) completely generic by extracting experiment-specific code (e.g., LHCb Bookkeeping integration) into plugins.

## Three-Level CWL Submission Model

```mermaid
flowchart TB
    subgraph "Level A: Production"
        P[dirac:Production hint]
        P --> IDP[Input Dataset Plugin]
        P --> |future| OC[Output Config]
        P --> |future| TD[Transformation Defaults]
    end

    subgraph "Level B: Transformation"
        T[dirac:Transformation hint]
        T --> TT[Transformation Type]
        T --> GS[Group Size]
        T --> TP[Plugin/Priority]
    end

    subgraph "Level C: Job"
        S[dirac:Scheduling hint]
        S --> Platform
        S --> Sites

        EH[dirac:ExecutionHooks hint]
        EH --> PreProcess
        EH --> PostProcess
        EH --> OutputStorage
    end

    P -.->|"configures input for"| T
    T -.->|"creates many"| J[Jobs]
    S -.->|"configures"| J
    EH -.->|"hooks into"| J
```

| Level | DIRAC Concept | CWL Scope | Hint |
|-------|---------------|-----------|------|
| **(a) Production** | Production/Request | Orchestrates transformations, configures input data | `dirac:Production` |
| **(b) Transformation** | Transformation | Job template - defines steps that run in each job | `dirac:Transformation` |
| **(c) Job** | Job | Single execution with specific inputs | `dirac:Scheduling`, `dirac:ExecutionHooks` |

**Key insight:** A Transformation workflow is a **job template**. DIRAC creates many jobs from it, each with different input files.

## Plugin Architecture

```mermaid
classDiagram
    class ProductionHint {
        +str input_dataset_plugin
        +dict input_dataset_config
        +from_cwl(cwl_content) ProductionHint
        +to_runtime() InputDatasetPluginBase
    }

    class InputDatasetPluginBase {
        <<abstract>>
        +ClassVar[str] vo
        +ClassVar[str] version
        +ClassVar[str] description
        +generate_inputs(workflow_path, config, ...) tuple[Path, Path]
        +format_hint_display(config) list[tuple]
    }

    class NoOpInputDatasetPlugin {
        +generate_inputs() tuple[None, None]
    }

    class LHCbBookkeepingPlugin {
        +ClassVar[str] vo = "lhcb"
        +generate_inputs() tuple[Path, Path]
        +format_hint_display() list[tuple]
    }

    class InputDatasetPluginRegistry {
        -dict _plugins
        +register_plugin(plugin_class)
        +discover_plugins() int
        +get_plugin(name) type
        +instantiate_plugin(hint) InputDatasetPluginBase
    }

    InputDatasetPluginBase <|-- NoOpInputDatasetPlugin
    InputDatasetPluginBase <|-- LHCbBookkeepingPlugin
    ProductionHint --> InputDatasetPluginRegistry : uses
    InputDatasetPluginRegistry --> InputDatasetPluginBase : manages
```

## CWL Hint Formats

### New Format: `dirac:Production`

```yaml
cwlVersion: v1.2
class: Workflow

hints:
  - class: dirac:Production
    input_dataset_plugin: "LHCbBookkeepingPlugin"
    input_dataset_config:
      event_type: "27165175"
      conditions_description: "Beam6800GeV-VeloClosed-MagUp-Excl-UT"
      conditions_dict:
        configName: "Collision24"
        configVersion: "Beam6800GeV-VeloClosed-MagUp-Excl-UT"
        inFileType: "BRUNELHLT2.DST"
        inProPass: "Sprucing24c5/DaVinciRestripping25r0"
```

## Data Flow

```mermaid
sequenceDiagram
    participant User
    participant CLI as __main__.py
    participant Registry as PluginRegistry
    participant Plugin as InputDatasetPlugin
    participant External as External System<br/>(e.g., LHCb Bookkeeping)
    participant CWLTool

    User->>CLI: dirac-cwl-run workflow.cwl
    CLI->>CLI: Parse CWL, extract hints
    CLI->>Registry: find_applicable_plugin(cwl_content)
    Registry->>Registry: Check dirac:Production
    Registry-->>CLI: LHCbBookkeepingPlugin instance

    CLI->>Plugin: generate_inputs(workflow_path, config, ...)
    Plugin->>External: Query for LFNs
    External-->>Plugin: LFN list
    Plugin->>External: Resolve replicas
    External-->>Plugin: Replica catalog data
    Plugin->>Plugin: Write inputs.yml
    Plugin->>Plugin: Write catalog.json
    Plugin-->>CLI: (inputs_path, catalog_path)

    Plugin-->>CLI: ["CMAKE_PREFIX_PATH", ...]

    CLI->>CWLTool: Execute with inputs + catalog
    CWLTool-->>CLI: Results
    CLI-->>User: Output
```

## File Structure

```
dirac-cwl/src/dirac_cwl/
├── production/
│   ├── __init__.py              # Package init + auto-discovery
│   ├── core.py                  # ProductionHint + InputDatasetPluginBase
│   ├── registry.py              # InputDatasetPluginRegistry
│   └── plugins/
│       ├── __init__.py          # Plugin exports
│       ├── core.py              # NoOpInputDatasetPlugin
│       └── lhcb.py              # LHCbBookkeepingPlugin (temporary)
├── job/
│   └── executor/
│       └── __main__.py          # Modified to use plugin system
└── ...
```

## Entry Points Configuration

In `pyproject.toml`:

```toml
[project.entry-points."dirac_cwl.input_dataset_plugins"]
NoOpInputDatasetPlugin = "dirac_cwl.production.plugins:NoOpInputDatasetPlugin"
LHCbBookkeepingPlugin = "dirac_cwl.production.plugins.lhcb:LHCbBookkeepingPlugin"
```

**Done so far (DIRACGrid/dirac-cwl#94):**
- `dirac:Production` hint with `input_dataset_plugin` for querying external catalogs
- Plugin generates `inputs.yml` + `catalog.json` for local CWL execution

**Future work (CWL submission tools):**
The following will be **configured via CWL hints** when we build the submission CLI, but are **executed by DIRAC agents** at runtime:

| Feature | CWL Hint | DIRAC Agent | Notes |
|---------|----------|-------------|-------|
| Transformation grouping | `dirac:Transformation` (plugin, groupSize) | TransformationAgent | Groups files into tasks |
| Input Meta Queries | `dirac:Transformation` (inputQuery) | InputDataAgent | Finds input files from upstream |
| Output registration | `dirac:ExecutionHooks` or similar | Job wrapper | Registers outputs to catalog |

The hints define **what** should happen; DIRAC agents execute **when** and **how**.
