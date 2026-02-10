# Schema Generation Documentation

This document describes the automatic schema generation system for DIRAC CWL metadata models.

## Overview

The DIRAC CWL prototype now automatically generates JSON and YAML schemas from Pydantic metadata models instead of using hardcoded schemas. This ensures that:

1. **Schemas are always in sync** with the actual Python models
2. **User plugins are automatically included** in the schema generation
3. **CWL hint validation** works with the latest model definitions
4. **Documentation** is auto-generated from model docstrings and field descriptions

## Schema Generation

### Automatic Generation (CI/CD)

Schemas are automatically generated in CI/CD when:
- Changes are made to metadata models in `src/dirac_cwl/metadata/`
- Changes are made to submission models in `src/dirac_cwl/submission_models.py`
- The schema generation script is modified

The CI workflow:
1. Detects changes to relevant files
2. Generates new schemas
3. Validates schema syntax
4. Commits and pushes updated schemas (on main branch)
5. Creates PR comments showing changes (on PRs)

### Manual Generation (Local Development)

```bash
# Generate all schemas (recommended)
make schemas

# Generate only JSON schemas
make schemas-json

# Generate only YAML schemas
make schemas-yaml

# Check if schemas are up to date
make check-schemas

# Validate schema syntax
make validate-schemas

# Clean generated schemas
make clean-schemas
```

### Direct Script Usage

```bash
# Basic usage
python scripts/generate_schemas.py

# Custom output directory and format
python scripts/generate_schemas.py \
    --output-dir my_schemas \
    --format yaml \
    --individual \
    --unified

# Generate only unified schema
python scripts/generate_schemas.py --unified --no-individual
```

## Generated Files

### Directory Structure

```
generated_schemas/
├── dirac-metadata.json          # Unified schema (JSON)
├── dirac-metadata.yaml          # Unified schema (YAML)
├── plugins-summary.json         # Plugin registry summary
└── individual/                  # Individual model schemas
    ├── Admin.json
    ├── User.json
    ├── LHCbSimulate.json
    ├── PiSimulate.json
    └── ...
```

### Schema Files

1. **`dirac-metadata.json`** - Main unified schema containing all metadata models
2. **`dirac-metadata.yaml`** - YAML version of the unified schema
3. **`plugins-summary.json`** - Summary of all registered plugins with metadata
4. **`individual/*.json`** - Individual schema files for each Pydantic model

## Using Generated Schemas

### In CWL Files

```yaml
$namespaces:
  dirac: "./schemas/dirac-metadata.json#"

hints:
  dirac:metadata:
    type: "LHCbSimulate"
    task_id: 123
    run_id: 456
  dirac:description:
    platform: "x86_64"
    priority: 10
```

### For Validation

```python
import json
import jsonschema

# Load schema
with open('generated_schemas/dirac-metadata.json') as f:
    schema = json.load(f)

# Validate data
data = {
    "metadata": {"type": "User"},
    "description": {"platform": "x86_64"}
}

jsonschema.validate(data, schema)
```

### For Documentation

The generated schemas include:
- Model descriptions from docstrings
- Field descriptions and types
- DIRAC-specific metadata (experiment, metadata_type)
- Validation constraints

## Adding New Metadata Models

### Creating a Plugin

1. **Create the model class**:
```python
# In src/dirac_cwl/metadata/plugins/my_experiment.py
from dirac_cwl.metadata.core import BaseMetadataModel

class MyMetadata(BaseMetadataModel):
    metadata_type: ClassVar[str] = "MyModel"
    description: ClassVar[str] = "Description of my metadata model"
    experiment: ClassVar[str] = "my_experiment"  # Optional

    # Your fields here
    parameter1: str
    parameter2: int = 42
```

2. **Schemas are generated automatically** - No manual schema writing needed!

3. **Test your model**:
```bash
# Generate schemas to include your new model
make schemas

# Verify it appears in the summary
cat generated_schemas/plugins-summary.json | jq '.plugins.MyModel'

# Test CWL workflows using your model
make test-schemas
```

### Plugin Discovery

The schema generation automatically discovers plugins by:
1. Scanning `dirac_cwl.metadata.plugins.*`
2. Looking for classes that inherit from `BaseMetadataModel`
3. Including experiment-specific namespaces
4. Supporting user-provided plugin packages

## Schema Validation in Development

### Pre-commit Hooks

Add to `.pre-commit-config.yaml`:
```yaml
- repo: local
  hooks:
    - id: check-schemas
      name: Check schemas are up to date
      entry: make check-schemas
      language: system
      pass_filenames: false
```

### IDE Integration

Most IDEs can use JSON schemas for autocompletion and validation:

1. **VS Code**: Add to `.vscode/settings.json`:
```json
{
  "yaml.schemas": {
    "./generated_schemas/dirac-metadata.json": [
      "**/metadata-*.yaml",
      "**/hints.yaml"
    ]
  }
}
```

2. **PyCharm**: Configure JSON schemas in Settings → Languages & Frameworks → Schemas and DTDs

## Troubleshooting

### Schema Generation Fails

1. **Check Pydantic model syntax**:
```bash
python -c "from dirac_cwl.metadata.plugins.my_plugin import MyModel"
```

2. **Check plugin registration**:
```bash
python -c "
from dirac_cwl.metadata import get_registry
registry = get_registry()
registry.discover_plugins()
print(registry.list_plugins())
"
```

3. **Verbose generation**:
```bash
python scripts/generate_schemas.py --verbose
```

### Schema Validation Errors

1. **Invalid JSON/YAML syntax** - Check the generation script output
2. **Pydantic model errors** - Some types can't be serialized to JSON Schema
3. **Missing dependencies** - Ensure all imports are available

### CI/CD Issues

1. **Permission errors** - Check GitHub token permissions
2. **Merge conflicts** - Manually resolve schema conflicts
3. **Test failures** - Update test data to match new schemas

## Migration from Hardcoded Schemas

The old hardcoded schemas in `src/dirac_cwl/schemas/` have been removed. To migrate:

1. **Update CWL files** to reference generated schemas:
```yaml
# Old
$namespaces:
  dirac: "package://dirac_cwl/schemas/dirac-metadata.yaml#"

# New
$namespaces:
  dirac: "./schemas/dirac-metadata.json#"
```

2. **Update validation code** to use generated schemas
3. **Remove hardcoded schema references** from your code

## Best Practices

1. **Always run `make schemas`** after modifying metadata models
2. **Include schema updates** in your commits when changing models
3. **Test CWL workflows** with generated schemas before merging
4. **Document model fields** - they become part of the schema documentation
5. **Use semantic versioning** for schema-breaking changes

---

For more information, see:
- [Pydantic JSON Schema documentation](https://docs.pydantic.dev/latest/usage/json_schema/)
- [JSON Schema specification](https://json-schema.org/)
- [CWL hints documentation](https://www.commonwl.org/user_guide/topics/metadata-and-authorship.html)
