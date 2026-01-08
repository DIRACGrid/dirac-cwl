#!/usr/bin/env python3
"""Generate JSON schemas from Pydantic metadata models.

This script automatically generates JSON schemas for all registered metadata
plugins in the DIRAC CWL prototype system. It includes schemas for:

1. Core metadata models (ExecutionHooksBasePlugin, MetadataDescriptor, TaskDescriptor)
2. Submission models (JobSubmissionModel, etc.)
3. All registered metadata plugins (user plugins included)

The generated schemas can be used for:
- CWL hint validation
- Documentation generation
- API validation
- IDE autocompletion

Usage:
    python scripts/generate_schemas.py [--output-dir OUTPUT_DIR] [--format json|yaml]
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, cast

import yaml

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def collect_pydantic_models() -> Dict[str, Any]:
    """Collect all Pydantic models from the metadata system."""
    models = {}

    # Import core models
    try:
        from dirac_cwl_proto.execution_hooks.core import (
            ExecutionHooksBasePlugin,
            ExecutionHooksHint,
            SchedulingHint,
            TransformationExecutionHooksHint,
        )

        models.update(
            {
                "ExecutionHooksBasePlugin": ExecutionHooksBasePlugin,
                "ExecutionHooks": ExecutionHooksHint,
                "Scheduling": SchedulingHint,
                "TransformationExecutionHooks": TransformationExecutionHooksHint,
            }
        )
        logger.info("Collected core metadata models")
    except ImportError as e:
        logger.error(f"Failed to import core models: {e}")

    # Import submission models
    try:
        from dirac_cwl_proto.submission_models import (
            JobInputModel,
            JobSubmissionModel,
            ProductionSubmissionModel,
            TransformationSubmissionModel,
        )

        models.update(
            {
                "JobInputModel": JobInputModel,
                "JobSubmissionModel": JobSubmissionModel,
                "TransformationSubmissionModel": TransformationSubmissionModel,
                "ProductionSubmissionModel": ProductionSubmissionModel,
            }
        )
        logger.info("Collected submission models")
    except ImportError as e:
        logger.error(f"Failed to import submission models: {e}")

    # Collect all registered metadata plugins
    try:
        from dirac_cwl_proto.execution_hooks import get_registry

        registry = get_registry()
        registry.discover_plugins()

        # Get all registered plugins
        all_experiments = registry.list_virtual_organizations()
        logger.info(f"Found experiments: {all_experiments}")

        for experiment in all_experiments + [None]:  # Include global plugins
            plugin_names = registry.list_plugins(experiment)
            for plugin_name in plugin_names:
                plugin_class = registry.get_plugin(plugin_name, experiment)
                if plugin_class:
                    key = f"{experiment}_{plugin_name}" if experiment else plugin_name
                    models[key] = plugin_class
                    logger.info(f"Collected plugin: {key} ({plugin_class.__name__})")

        excluded_prefixes = (
            "BaseMetadata",
            "Job",
            "Transformation",
            "Production",
        )
        plugin_count = len([k for k in models.keys() if not k.startswith(excluded_prefixes)])
        logger.info(f"Collected {plugin_count} metadata plugins")
    except ImportError as e:
        logger.error(f"Failed to import metadata registry: {e}")

    return models


def create_cwl_schema_shim(model_class: Any, model_name: str) -> Dict[str, Any]:
    """Create a schema shim for models containing CWL types."""
    # Create a custom schema that references CWL schemas and handles the fields we can
    schema = {
        "title": model_name,
        "description": f"Schema for {model_name} with CWL type references",
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }

    # Get the model fields using Pydantic's field info
    for field_name, field_info in model_class.model_fields.items():
        annotation = field_info.annotation

        # Handle CWL Workflow type specifically
        if hasattr(annotation, "__name__") and getattr(annotation, "__name__", None) == "Workflow":
            properties = cast(Dict[str, Any], schema["properties"])
            properties[field_name] = {
                "description": "CWL Workflow definition",
                "$ref": "https://json.schemastore.org/cwl-workflow.json",
            }
        # Handle other CWL types
        elif hasattr(annotation, "__module__") and "cwl_utils.parser" in str(getattr(annotation, "__module__", "")):
            annotation_name = getattr(annotation, "__name__", "Unknown")
            properties = cast(Dict[str, Any], schema["properties"])
            properties[field_name] = {
                "description": f"CWL {annotation_name} definition",
                "$ref": "https://json.schemastore.org/cwlc.json",
            }
        # Handle dict types
        elif hasattr(annotation, "__origin__") and getattr(annotation, "__origin__", None) is dict:
            # Try to get the value type for dict[str, SomeType]
            args = getattr(annotation, "__args__", [])
            if len(args) == 2:
                value_type = args[1]
                value_type_name = getattr(value_type, "__name__", None)
                if value_type_name:
                    properties = cast(Dict[str, Any], schema["properties"])
                    properties[field_name] = {
                        "type": "object",
                        "description": f"Dictionary mapping strings to {value_type_name}",
                        "additionalProperties": {"$ref": f"#/$defs/{value_type_name}"},
                    }
                else:
                    properties = cast(Dict[str, Any], schema["properties"])
                    properties[field_name] = {
                        "type": "object",
                        "description": "Dictionary with string keys",
                    }
            else:
                properties = cast(Dict[str, Any], schema["properties"])
                properties[field_name] = {
                    "type": "object",
                    "description": "Dictionary",
                }
        else:
            # For other types, create a generic schema
            properties = cast(Dict[str, Any], schema["properties"])
            properties[field_name] = {"description": f"Field of type {annotation}"}

        # Add to required if the field is required
        if field_info.is_required():
            required_list = cast(List[str], schema["required"])
            required_list.append(field_name)

    return schema


def generate_schema(model_class: Any, model_name: str) -> Dict[str, Any]:
    """Generate JSON schema for a Pydantic model."""
    try:
        schema = model_class.model_json_schema()

        # Add metadata if available
        if hasattr(model_class, "description"):
            schema["dirac_description"] = model_class.description
        if hasattr(model_class, "vo"):
            schema["dirac_vo"] = model_class.vo
        if hasattr(model_class, "get_hook_plugin"):
            schema["dirac_hook_plugin"] = model_class.name()

        # Set title if not present
        if "title" not in schema:
            schema["title"] = model_name

        return schema
    except Exception as e:
        # Handle CWL-related models specially
        if "cwl_utils.parser" in str(e) or "IsInstanceSchema" in str(e) or "Workflow" in str(e):
            logger.info(f"Creating CWL schema reference for {model_name}")
            return create_cwl_schema_shim(model_class, model_name)

        logger.warning(f"Skipping schema generation for {model_name}: {e}")
        return {}


def generate_unified_dirac_schema(models: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a unified DIRAC metadata schema that references all plugins."""
    # Base schema structure
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "DIRAC Metadata Schema",
        "description": "Complete schema for DIRAC CWL metadata including all registered plugins",
        "type": "object",
        "properties": {
            "metadata": {
                "type": "object",
                "description": "Metadata descriptor for the task",
                "allOf": [{"$ref": "#/$defs/MetadataDescriptor"}],
            },
            "description": {
                "type": "object",
                "description": "Task description and execution parameters",
                "allOf": [{"$ref": "#/$defs/TaskDescriptor"}],
            },
        },
        "additionalProperties": False,
        "$defs": {},
    }

    # Add all model schemas to definitions
    for name, model_class in models.items():
        model_schema = generate_schema(model_class, name)
        if model_schema:  # Only add schemas that were successfully generated
            defs = cast(Dict[str, Any], schema["$defs"])
            defs[name] = model_schema

    return schema


def save_schema(schema: Dict[str, Any], output_path: Path, format: str = "json") -> None:
    """Save schema to file in specified format."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if format.lower() == "json":
        with open(output_path, "w") as f:
            json.dump(schema, f, indent=2, sort_keys=True)
    elif format.lower() == "yaml":
        with open(output_path, "w") as f:
            yaml.dump(schema, f, default_flow_style=False, sort_keys=True)
    else:
        raise ValueError(f"Unsupported format: {format}")

    logger.info(f"Saved schema to {output_path}")


def main():
    """Generate schemas from Pydantic metadata models."""
    parser = argparse.ArgumentParser(description="Generate JSON schemas from Pydantic metadata models")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("generated_schemas"),
        help="Output directory for generated schemas (default: generated_schemas/)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "yaml"],
        default="json",
        help="Output format for schemas (default: json)",
    )
    parser.add_argument(
        "--individual",
        action="store_true",
        help="Generate individual schema files for each model",
    )
    parser.add_argument(
        "--unified",
        action="store_true",
        default=True,
        help="Generate unified DIRAC schema (default: True)",
    )

    args = parser.parse_args()

    logger.info("Starting schema generation...")

    # Collect all models
    models = collect_pydantic_models()
    if not models:
        logger.error("No models found. Exiting.")
        return 1

    logger.info(f"Found {len(models)} models to process")

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Generate individual schemas if requested
    if args.individual:
        individual_dir = args.output_dir / "individual"
        individual_dir.mkdir(exist_ok=True)

        for name, model_class in models.items():
            schema = generate_schema(model_class, name)
            if schema:
                output_file = individual_dir / f"{name}.{args.format}"
                save_schema(schema, output_file, args.format)

    # Generate unified schema
    if args.unified:
        unified_schema = generate_unified_dirac_schema(models)
        unified_file = args.output_dir / f"dirac-metadata.{args.format}"
        save_schema(unified_schema, unified_file, args.format)

    # Generate plugin summary
    plugin_models = {k: v for k, v in models.items() if hasattr(v, "name")}
    if plugin_models:
        summary = {
            "plugins": {
                name: {
                    "class": model_class.__name__,
                    "hook_plugin": (model_class.name() if hasattr(model_class, "get_hook_plugin") else None),
                    "description": getattr(model_class, "description", None),
                    "vo": getattr(model_class, "vo", None),
                    "module": model_class.__module__,
                }
                for name, model_class in plugin_models.items()
            },
            "total_plugins": len(plugin_models),
            "virtual_organizations": list(
                set(
                    getattr(m, "vo", None)
                    for m in plugin_models.values()
                    if hasattr(m, "vo") and getattr(m, "vo", None)
                )
            ),
        }
        summary_file = args.output_dir / f"plugins-summary.{args.format}"
        save_schema(summary, summary_file, args.format)

    logger.info("Schema generation completed successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
