#!/usr/bin/env python3
"""Wrapper for lb-prod-run that handles CWL inputs and configuration merging."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import tarfile
import time
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import requests
from diracx.core.models.replica_map import ReplicaMap

# Configure logger to use UTC time
logger = logging.getLogger("run_lbprodrun")
logging.Formatter.converter = time.gmtime  # Use UTC for all log timestamps
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("%(asctime)s UTC - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(_handler)
logger.setLevel(logging.INFO)
logger.propagate = False


def analyse_xml_summary(xml_path: Path) -> bool:
    """Analyse XML summary file for errors.

    Checks that:
    - success is True
    - step is finalize
    - all input files have status="full"
    - all output files are present

    :param xml_path: Path to the XML summary file
    :return: True if analysis passes, False otherwise
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Check success flag
        success = root.find("success")
        if success is None or success.text != "True":
            logger.error("Success flag is not True: %s", success.text if success is not None else "missing")
            return False

        # Check step
        step = root.find("step")
        if step is None or step.text != "finalize":
            logger.error("Step is not 'finalize': %s", step.text if step is not None else "missing")
            return False

        # Check input files - all should have status="full"
        input_section = root.find("input")
        if input_section is not None:
            input_files = input_section.findall("file")
            for inp_file in input_files:
                status = inp_file.get("status", "unknown")
                name = inp_file.get("name", "unknown")
                if status != "full":
                    logger.error("Input file '%s' has status '%s' (expected 'full')", name, status)
                    return False

        # Check output files
        output_section = root.find("output")
        if output_section is not None:
            output_files = output_section.findall("file")

            # Log big warning if no output files
            if len(output_files) == 0:
                logger.warning("No output files found in XML summary. This may indicate:")
                logger.warning("  - Input files had no events matching the selection criteria")
                logger.warning("  - Configuration issue preventing output file creation")
                logger.warning("  - Application error that was not caught")
                logger.warning("  - NTuples written but not reported in XML summary")

            # Check all output files have status="full"
            for out_file in output_files or []:
                status = out_file.get("status", "unknown")
                name = out_file.get("name", "unknown")
                if status != "full":
                    logger.error("Output file '%s' has status '%s' (expected 'full')", name, status)
                    return False

        return True

    except ET.ParseError as e:
        logger.error("Failed to parse XML summary: %s", e)
        return False
    except Exception as e:
        logger.error("Error analyzing XML summary: %s", e)
        return False


def generate_pool_xml_catalog_from_replica_map(replica_map_path: Path, output_path: Path) -> None:
    """Generate a pool_xml_catalog.xml from a replica_map.json.

    The pool XML catalog format is used by LHCb applications to locate input files.
    This function converts our JSON replica map format to the XML format.

    :param replica_map_path: Path to replica_map.json
    :param output_path: Path where pool_xml_catalog.xml will be written
    """
    # Load replica map
    replica_map = ReplicaMap.model_validate_json(replica_map_path.read_text())

    # Create XML structure
    # <?xml version="1.0" encoding="UTF-8" standalone="no" ?>
    # <!-- Edited By POOL -->
    # <!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
    # <POOLFILECATALOG>
    #   <File ID="guid">
    #     <physical>
    #       <pfn filetype="type" name="pfn_url"/>
    #     </physical>
    #     <logical>
    #       <lfn name="lfn_path"/>
    #     </logical>
    #   </File>
    # </POOLFILECATALOG>

    root = ET.Element("POOLFILECATALOG")

    for lfn, entry in replica_map.root.items():
        # Create File element with GUID as ID
        file_elem = ET.SubElement(root, "File")
        if entry.checksum and entry.checksum.guid:
            file_elem.set("ID", entry.checksum.guid)

        # Physical section
        physical = ET.SubElement(file_elem, "physical")
        for replica in entry.replicas:
            pfn = ET.SubElement(physical, "pfn")
            # Convert URL to string - handle both str and URL types
            pfn_url = str(replica.url)
            pfn.set("name", pfn_url)
            # Optionally add filetype if we can determine it from LFN
            # For now, we'll leave it empty or add based on extension
            filetype = _guess_filetype(lfn)
            if filetype:
                pfn.set("filetype", filetype)

        # Logical section
        logical = ET.SubElement(file_elem, "logical")
        lfn_elem = ET.SubElement(logical, "lfn")
        lfn_elem.set("name", lfn)

    # Create the tree and write with proper formatting
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")  # Pretty print with 2-space indentation

    # Write XML with declaration
    with open(output_path, "wb") as f:
        f.write(b'<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
        f.write(b"<!-- Edited By POOL -->\n")
        f.write(b'<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n')
        tree.write(f, encoding="UTF-8", xml_declaration=False)

    logger.info("Generated pool_xml_catalog.xml with %d entries", len(replica_map.root))


def _guess_filetype(lfn: str) -> str:
    """Guess the LHCb file type from LFN extension.

    Common LHCb file types:
    - SIM, DIGI, DST, MDST, RDST, RAW, etc.
    """
    lfn_lower = lfn.lower()

    # Extract extension
    if "." in lfn_lower:
        ext = lfn_lower.split(".")[-1]

        # Map extensions to file types
        type_map = {
            "sim": "SIM",
            "digi": "DIGI",
            "dst": "DST",
            "mdst": "MDST",
            "rdst": "RDST",
            "raw": "RAW",
            "xdst": "XDST",
            "ldst": "LDST",
        }

        return type_map.get(ext, ext.upper())

    return ""


def add_output_files_to_replica_map(
    replica_map_path: Path,
    output_prefix: str,
    output_types: list[str],
    working_dir: Path = Path("."),
) -> None:
    """Add output files produced by lb-prod-run to the replica map.

    lb-prod-run creates output files but doesn't add LFNs to pool XML.
    We need to scan for output files and add them to the replica map
    with generated LFNs.

    :param replica_map_path: Path to replica_map.json
    :param output_prefix: Output file prefix (e.g., "00012345_00006789_1")
    :param output_types: List of output file types (e.g., ["SIM", "DIGI"])
    :param working_dir: Directory to search for output files
    """
    import uuid

    # Load existing replica map
    if replica_map_path.exists():
        replica_map = ReplicaMap.model_validate_json(replica_map_path.read_text())
    else:
        replica_map = ReplicaMap(root={})

    new_count = 0

    # For each output type, find matching files
    for output_type in output_types:
        # Search for files matching the pattern
        # LHCb files are typically: {prefix}.{lowercase_type}
        # e.g., 00012345_00006789_1.sim, 00012345_00006789_2.digi
        pattern = f"{output_prefix}.{output_type.lower()}"

        matching_files = list(working_dir.glob(pattern))

        # Also try case-insensitive patterns
        if not matching_files:
            for ext_variant in [
                output_type.lower(),
                output_type.upper(),
                output_type.title(),
            ]:
                matching_files = list(working_dir.glob(f"{output_prefix}.{ext_variant}"))
                if matching_files:
                    break

        for local_file in matching_files:
            if not local_file.is_file():
                continue

            # Generate LFN for this file
            # Format: LFN:{filename}
            lfn = f"LFN:{local_file.name}"

            # Get file size
            file_size = local_file.stat().st_size

            # Generate GUID
            file_guid = str(uuid.uuid4())

            # Create replica entry
            pfn_url = f"file://{local_file.resolve()}"
            replica = ReplicaMap.MapEntry.Replica(url=pfn_url, se="DIRAC.Client.Local")
            checksum = ReplicaMap.MapEntry.Checksum(guid=file_guid)

            entry = ReplicaMap.MapEntry(replicas=[replica], checksum=checksum, size_bytes=file_size)

            replica_map.root[lfn] = entry
            new_count += 1
            logger.info("  Added to replica map: %s -> %s (%d bytes)", lfn, local_file.name, file_size)

    # Write updated replica map
    replica_map_path.write_text(replica_map.model_dump_json(indent=2))

    if new_count > 0:
        logger.info("Added %d output file(s) to replica map", new_count)
    else:
        logger.info("No output files found to add to replica map")


def update_pool_xml_to_absolute_paths(pool_xml_path: Path) -> int:
    """Update all relative PFN paths in pool XML catalog to absolute paths.

    :param pool_xml_path: Path to the pool XML catalog file
    :return: Number of PFNs updated
    """
    if not pool_xml_path.exists():
        logger.warning("Pool XML file %s does not exist.", pool_xml_path)
        return 0

    # Parse the XML
    tree = ET.parse(pool_xml_path)
    root = tree.getroot()

    updated_count = 0
    # Find all pfn elements
    for pfn_elem in root.findall(".//pfn"):
        pfn_name = pfn_elem.get("name")
        if pfn_name:
            pfn_path = Path(pfn_name)
            # Only update if it's not already an absolute path
            if not pfn_path.is_absolute():
                # Check if file exists in current directory
                if pfn_path.exists():
                    absolute_path = pfn_path.resolve().as_posix()
                    pfn_elem.set("name", absolute_path)
                    logger.info("  Updated: %s -> %s", pfn_name, absolute_path)
                    updated_count += 1
                else:
                    logger.warning("File %s not found in current directory", pfn_name)

    # Write back the updated XML
    # Preserve the XML declaration and DOCTYPE
    tree.write(pool_xml_path, encoding="UTF-8", xml_declaration=True)

    # Fix the DOCTYPE manually since ElementTree doesn't preserve it well
    with open(pool_xml_path, "r") as f:
        content = f.read()

    # Add the POOLFILECATALOG comment and DOCTYPE after the XML declaration
    if "<!DOCTYPE POOLFILECATALOG" not in content:
        content = content.replace(
            "<?xml version='1.0' encoding='UTF-8'?>",
            '<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n'
            "<!-- Edited By POOL -->\n"
            '<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">',
        )
        with open(pool_xml_path, "w") as f:
            f.write(content)

    logger.info("Pool XML file updated. %d PFN(s) converted to absolute paths.", updated_count)
    return updated_count


def update_replica_map_from_pool_xml(pool_xml_path: Path, replica_map_path: Path) -> None:
    """Update replica map with changes from pool_xml_catalog.xml.

    This function reads the pool XML catalog (which may have been updated by lb-prod-run)
    and propagates any changes back to the replica map JSON file. This ensures
    new output files and their metadata are captured in the replica map.

    :param pool_xml_path: Path to pool_xml_catalog.xml
    :param replica_map_path: Path to replica_map.json to update
    """
    # Load existing replica map if it exists
    if replica_map_path.exists():
        replica_map = ReplicaMap.model_validate_json(replica_map_path.read_text())
    else:
        replica_map = ReplicaMap(root={})

    # Parse pool XML catalog
    tree = ET.parse(pool_xml_path)
    root = tree.getroot()

    updated_count = 0
    new_count = 0

    # Process each File element in the XML
    for file_elem in root.findall(".//File"):
        guid = file_elem.get("ID", "")

        # Extract LFN from logical section
        lfn_elem = file_elem.find(".//logical/lfn")
        lfn = lfn_elem.get("name", "") if lfn_elem is not None else None

        # Extract PFNs from physical section
        pfn_elems = file_elem.findall(".//physical/pfn")
        replicas = []
        for pfn_elem in pfn_elems:
            pfn_url = pfn_elem.get("name", "")
            if pfn_url:
                # Convert file:// paths or absolute paths to proper URLs
                if not pfn_url.startswith(("file://", "http://", "https://", "root://", "xroot://")):
                    # Assume it's a local absolute path
                    pfn_url = f"file://{pfn_url}" if pfn_url.startswith("/") else f"file://{Path(pfn_url).resolve()}"

                    if not lfn:
                        # If LFN is missing, generate one from filename
                        lfn = f"{Path(pfn_url).name}"

                replicas.append(ReplicaMap.MapEntry.Replica(url=pfn_url, se="DIRAC.Client.Local"))

        if not replicas or not lfn:
            continue

        # Get file size if the file exists locally
        file_size = None
        for replica in replicas:
            if str(replica.url).startswith("file://"):
                local_path = Path(str(replica.url).replace("file://", ""))
                if local_path.exists():
                    file_size = local_path.stat().st_size
                    break

        # Create or update entry in replica map
        if lfn in replica_map.root:
            # Update existing entry
            entry = replica_map.root[lfn]
            # Merge replicas (avoid duplicates)
            existing_urls = {str(r.url) for r in entry.replicas}
            for replica in replicas:
                if str(replica.url) not in existing_urls:
                    entry.replicas.append(replica)

            # Update GUID if we have one and it's different
            if guid:
                if entry.checksum is None:
                    entry.checksum = ReplicaMap.MapEntry.Checksum(guid=guid)
                elif entry.checksum.guid != guid:
                    entry.checksum.guid = guid

            # Update file size if we calculated it
            if file_size is not None and entry.size_bytes != file_size:
                entry.size_bytes = file_size

            updated_count += 1
        else:
            # Create new entry
            checksum = ReplicaMap.MapEntry.Checksum(guid=guid) if guid else None
            entry = ReplicaMap.MapEntry(replicas=replicas, checksum=checksum, size_bytes=file_size)
            replica_map.root[lfn] = entry
            new_count += 1

    # Write updated replica map back to file
    replica_map_path.write_text(replica_map.model_dump_json(indent=2))

    logger.info("Replica map updated: %d new entries, %d updated entries", new_count, updated_count)
    logger.info("Total entries in replica map: %d", len(replica_map.root))


def write_debug_script(output_prefix: str, working_dir: Path) -> None:
    """Write a shell script to reproduce the command for interactive debugging.

    :param output_prefix: Output file prefix (for naming the script)
    :param working_dir: Current working directory
    """
    import os
    import shlex

    script_path = working_dir / f"debug_interactive_{output_prefix}.sh"

    # Get current environment variables
    cmake_prefix = os.environ.get("CMAKE_PREFIX_PATH", "")
    analysis_prods_dynamic = os.environ.get("ANALYSIS_PRODUCTIONS_DYNAMIC", "")

    # Reconstruct the command from sys.argv, adding --interactive if not present
    cmd_parts = sys.argv.copy()

    # Add --interactive if not already present
    if "--interactive" not in cmd_parts:
        cmd_parts.append("--interactive")

    # Write the shell script
    script_content = "#!/bin/bash\n"
    script_content += "# Auto-generated debug script for interactive CWL debugging\n"
    script_content += f"# Generated from command: {' '.join(sys.argv)}\n"
    script_content += f"# Working directory: {working_dir}\n\n"

    script_content += "# Change to working directory\n"
    script_content += f'cd "{working_dir}"\n\n'

    script_content += "# Set environment variables\n"
    if cmake_prefix:
        script_content += f'export CMAKE_PREFIX_PATH="{cmake_prefix}"\n'
    if analysis_prods_dynamic:
        script_content += f'export ANALYSIS_PRODUCTIONS_DYNAMIC="{analysis_prods_dynamic}"\n'
    script_content += "\n"

    script_content += "# Run the interactive command\n"
    # Properly quote arguments with spaces
    quoted_cmd = " ".join(shlex.quote(arg) for arg in cmd_parts)
    script_content += quoted_cmd + "\n"

    # Write script
    script_path.write_text(script_content)
    script_path.chmod(0o755)  # Make executable

    logger.info("Debug script written to: %s", script_path)
    logger.info("   Run with: bash %s", script_path)


def main():
    """Run lbprodrun wrapper for DIRAC CWL."""
    parser = argparse.ArgumentParser(description="LbProdRun Wrapper for DIRAC CWL")
    parser.add_argument("config_file", help="Base configuration JSON file")
    parser.add_argument(
        "--input-files",
        help="Input paths that are resolved from direct local file paths (txt file)",
    )
    parser.add_argument(
        "--pool-xml-catalog",
        default="pool_xml_catalog.xml",
        help="Pool XML catalog file",
    )
    parser.add_argument(
        "--replica-map",
        help="Replica map JSON file (generates pool XML catalog if provided)",
    )
    parser.add_argument("--run-number", type=int, help="Run number")
    parser.add_argument("--first-event-number", type=int, help="First event number")
    parser.add_argument("--number-of-events", type=int, help="Number of events")
    parser.add_argument("--number-of-processors", type=int, help="Number of processors")
    parser.add_argument("--output-prefix", help="Output file prefix")
    parser.add_argument("--event-type", help="Event type ID for Gauss")
    parser.add_argument("--histogram", action="store_true", help="Enable histogram output")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")

    args = parser.parse_args()

    # Generate pool_xml_catalog from replica_map if provided
    if args.replica_map:
        replica_map_path = Path(args.replica_map)
        if replica_map_path.exists():
            logger.info("Generating pool_xml_catalog.xml from %s...", replica_map_path)
            try:
                generate_pool_xml_catalog_from_replica_map(replica_map_path, Path(args.pool_xml_catalog))
            except Exception as e:
                logger.warning("Failed to generate pool XML catalog from replica map: %s", e)
                logger.warning("   Will proceed without pool XML catalog")
        else:
            logger.warning("Replica map %s not found", replica_map_path)

    # Load base configuration
    config = json.loads(Path(args.config_file).read_text())

    # Merge command-line arguments
    if args.number_of_events is not None:
        config["input"]["n_of_events"] = args.number_of_events

    if args.number_of_processors is not None:
        config["application"]["number_of_processors"] = args.number_of_processors

    if args.output_prefix:
        config["output"]["prefix"] = args.output_prefix

    if args.run_number is not None:
        config["input"]["run_number"] = args.run_number

    if args.first_event_number is not None:
        config["input"]["first_event_number"] = args.first_event_number

    if args.histogram:
        config["output"]["histogram"] = True

    if args.pool_xml_catalog:
        config["input"]["xml_file_catalog"] = Path(args.pool_xml_catalog).name

    if args.input_files:
        paths = Path(args.input_files).read_text().splitlines()
        config["input"]["files"] = [path.strip() for path in paths]

    # check the options files for @{eventType} if application is Gauss
    if config["application"]["name"].lower() == "gauss":
        options = config["options"].get("files", [])
        if isinstance(options, list):
            if not [opt for opt in options if "@{eventType}" in opt]:
                raise ValueError(
                    "For Gauss, at least one option file path must contain the '@{eventType}' placeholder."
                )
        if args.event_type is None:
            raise ValueError("Event type ID must be provided for Gauss application.")
        # substitute event type in options
        config["options"]["files"] = [opt.replace("@{eventType}", args.event_type) for opt in options]

    app_name = config["application"]["name"]
    cleaned_appname = app_name.replace("/", "").replace(" ", "")

    # Set XML summary file name
    xml_summary_filename = f"summary{cleaned_appname}_{args.output_prefix}.xml"
    config["input"]["xml_summary_file"] = xml_summary_filename

    # Write merged configuration to prodConf file
    config_filename = f"prodConf_{cleaned_appname}_{args.output_prefix}.json"
    output_config = Path(config_filename)
    output_config.write_text(json.dumps(config, indent=2))

    # Write debug script before running (so it's available even if run fails)
    write_debug_script(
        output_prefix=args.output_prefix,
        working_dir=Path.cwd(),
    )

    # Run lb-prod-run with the merged configuration
    returncode, _, _ = asyncio.run(
        run_lbprodrun(
            application_log=f"{cleaned_appname}_{args.output_prefix}.log",
            prodconf_file=config_filename,
            interactive=args.interactive,
        )
    )

    # Check the summary XML for errors
    xml_summary_path = Path(xml_summary_filename)
    if xml_summary_path.exists():
        logger.info("Analyzing XML summary: %s", xml_summary_filename)
        is_ok = analyse_xml_summary(xml_summary_path)

        if not is_ok:
            logger.error("XML Summary analysis failed for %s", xml_summary_filename)
            logger.error("The application reported errors during execution.")
            sys.exit(1)
        else:
            logger.info("XML Summary analysis passed for %s", xml_summary_filename)
    else:
        logger.warning("XML summary file not found: %s", xml_summary_filename)

    # Check if any output exists for each filetype
    # {output_prefix}.{filetype}
    for filetype_expected in config["output"].get("types", []):
        expected_filename = f"{args.output_prefix}.{filetype_expected.lower()}"
        if not Path(expected_filename).exists():
            logger.error("Expected output file not found: %s (filetype: %s)", expected_filename, filetype_expected)
        else:
            logger.info("Output file found: %s", expected_filename)

    # Update all relative PFN paths in the pool XML catalog to absolute paths
    logger.info("Updating Pool XML file...")
    catalog_path = Path(args.pool_xml_catalog)
    update_pool_xml_to_absolute_paths(catalog_path)

    # Update replica map if it was provided
    if args.replica_map and catalog_path.exists():
        logger.info("Updating replica map from pool XML...")
        try:
            update_replica_map_from_pool_xml(catalog_path, Path(args.replica_map))
        except Exception as e:
            logger.warning("Failed to update replica map: %s", e)
    sys.exit(returncode)


def download_franklin_from_mr(job_dir: Path, mr_id: str) -> str | None:
    """Download Franklin artifacts from GitLab MR."""
    CI_PROJECT_ID = 210208  # Franklin project ID hardcoded

    print(f"Downloading Franklin from MR {mr_id}...")

    # Create directory structure
    fake_site = job_dir / "fake-site"
    franklin_base = fake_site / "FRANKLIN"
    franklin_base.mkdir(parents=True, exist_ok=True)

    try:
        # Get pipelines for this MR
        mr_pipelines_url = f"https://gitlab.cern.ch/api/v4/projects/{CI_PROJECT_ID}/merge_requests/{mr_id}/pipelines"
        pipelines_response = requests.get(mr_pipelines_url, timeout=30)  # Is it 200, 404 etc

        if pipelines_response.status_code != 200:
            print(f"Failed to get pipelines for MR {mr_id}: {pipelines_response.status_code}")
            return None

        # Find the most recent successful pipeline
        target_pipeline_id = None
        for pipeline in pipelines_response.json():
            if pipeline.get("status") == "success":
                target_pipeline_id = pipeline["id"]
                print(f"Found successful pipeline: {target_pipeline_id}")
                break

        if not target_pipeline_id:
            print(f"No successful pipeline found for MR {mr_id}")
            return None

        # Get the merge_builds_and_source job
        jobs_url = f"https://gitlab.cern.ch/api/v4/projects/{CI_PROJECT_ID}/pipelines/{target_pipeline_id}/jobs"
        jobs_response = requests.get(jobs_url, timeout=30)

        if jobs_response.status_code != 200:
            print(f"Failed to get jobs: {jobs_response.status_code}")
            return None

        merge_job_id = None
        for job in jobs_response.json():
            if job["name"] == "merge_builds_and_source" and job["status"] == "success":
                merge_job_id = job["id"]
                print(f"Found merge job: {merge_job_id}")
                break

        if not merge_job_id:
            print("No successful merge_builds_and_source job found")
            return None

        # Download artifacts
        artifacts_url = f"https://gitlab.cern.ch/api/v4/projects/{CI_PROJECT_ID}/jobs/{merge_job_id}/artifacts"
        artifacts_response = requests.get(artifacts_url, timeout=300)

        if artifacts_response.status_code != 200:
            print(f"Failed to download artifacts: {artifacts_response.status_code}")
            return None

        print(f"Downloaded Franklin artifacts ({len(artifacts_response.content)} bytes)")

        # Extract Franklin build from artifacts
        target_file = f"FRANKLIN_v1r{mr_id}.tar.gz"
        zip_file = fake_site / "franklin_artifacts.zip"

        # Save and extract zip
        with open(zip_file, "wb") as f:
            f.write(artifacts_response.content)

        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            if target_file not in zip_ref.namelist():
                print(f"{target_file} not found in artifacts")
                zip_file.unlink()  # Cleanup on failure
                return None

            # Extract tar.gz from zip
            zip_ref.extract(target_file, fake_site)

        # Extract Franklin from tar.gz
        with tarfile.open(fake_site / target_file, "r:gz") as tar:
            tar.extractall(franklin_base, filter="data")

        # Cleanup temporary files
        (fake_site / target_file).unlink()
        zip_file.unlink()

        print(f"Franklin MR {mr_id} extracted successfully")
        # version = f"v1r{mr_id}"
        # current_cmake = os.environ.get("CMAKE_PREFIX_PATH", "")
        # job_cmake_path = (
        #     f"{current_cmake}:{str(fake_site)}" if current_cmake else str(fake_site)
        # )
        return str(fake_site.resolve())

        # if validate_franklin_setup(str(fake_site), version, job_cmake_path):
        # else:
        #     print(f"Franklin setup validation failed for {version}")
        #     return None
    except Exception as e:
        print(f"Error downloading Franklin: {e}")
        return None


def check_and_setup_franklin(job_dir: Path, prodconf_file: Path) -> tuple[dict[str, str], bool]:
    """Check if Franklin is needed and set it up if required."""
    extra_env: dict[str, str] = {}
    franklin_downloaded = False
    try:
        prodconf_data = json.loads(prodconf_file.read_text())
    except Exception:
        return extra_env, franklin_downloaded

    found_versions: list[str] = []

    app_name = prodconf_data["application"].get("name", "")
    # application must be a dict and name must be Franklin
    if app_name != "Franklin":
        return extra_env, franklin_downloaded
    version = prodconf_data["application"].get("version", "")
    found_versions.append(version)

    if not found_versions:
        return extra_env, franklin_downloaded

    version = found_versions[0]

    if not version.startswith("v1r"):
        return extra_env, franklin_downloaded

    mr_id = version[3:]
    cvmfs_path = f"/cvmfs/lhcb.cern.ch/lib/lhcb/FRANKLIN/FRANKLIN_{version}"
    if os.path.exists(cvmfs_path):
        return extra_env, franklin_downloaded

    franklin_path = download_franklin_from_mr(job_dir, mr_id)
    if not franklin_path:
        raise RuntimeError(f"Failed to download unreleased Franklin MR {mr_id}")

    current_cmake = os.environ.get("CMAKE_PREFIX_PATH", "")
    new_cmake = f"{current_cmake}:{franklin_path}" if current_cmake else franklin_path
    extra_env["CMAKE_PREFIX_PATH"] = new_cmake
    print(f"Setting CMAKE_PREFIX_PATH to: {new_cmake}")
    franklin_downloaded = True
    return extra_env, franklin_downloaded


async def run_lbprodrun(
    application_log: str,
    prodconf_file: str,
    interactive: bool = False,
) -> tuple[int, str, str]:
    """Run the application using lb-prod-run."""
    import os

    # Debug: Check if CMAKE_PREFIX_PATH is set
    cmake_prefix = os.environ.get("CMAKE_PREFIX_PATH", "NOT SET")
    analysis_prods_dynamic = os.environ.get("ANALYSIS_PRODUCTIONS_DYNAMIC", "NOT SET")
    logger.info("DEBUG: CMAKE_PREFIX_PATH = %s", cmake_prefix)
    logger.info("DEBUG: ANALYSIS_PRODUCTIONS_DYNAMIC = %s", analysis_prods_dynamic)

    os.environ["LBPRODRUN_PRMON_INTERVAL"] = "1"
    # Force unbuffered stdout/stderr for all Python processes in the chain.
    # lb-prod-run uses os.execvpe() which doesn't flush Python's IO buffers,
    # and the child Gaudi process uses block buffering for piped stdout.
    os.environ["PYTHONUNBUFFERED"] = "1"

    extra_env, franklin_downloaded = check_and_setup_franklin(Path(".").resolve(), Path(prodconf_file))
    if franklin_downloaded:
        for key, value in extra_env.items():
            os.environ[key] = value

    # Build command with optional --interactive flag
    interactive_args = ["--interactive"] if interactive else []
    command = ["lb-prod-run", *interactive_args, "--prmon", "--verbose", prodconf_file]

    stdout = ""
    stderr = ""

    # In interactive mode, connect stdin/stdout/stderr directly to terminal
    if interactive:
        proc = await asyncio.create_subprocess_exec(
            *command,
            stdin=None,  # Inherit from parent (the terminal)
            stdout=None,  # Inherit from parent
            stderr=None,  # Inherit from parent
        )
        # In interactive mode, just wait for process to complete
        # No output handling needed since it goes directly to terminal
        returncode = await proc.wait()
        return (returncode, stdout, stderr)
    else:
        # Non-interactive mode: capture and log output
        proc = await asyncio.create_subprocess_exec(
            *command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout_fh = open(application_log, "a")
        stderr_fh = stdout_fh

        try:
            # Type check: ensure streams exist before handling
            if proc.stdout is None or proc.stderr is None:
                raise RuntimeError("Process streams are None in non-interactive mode")

            await asyncio.gather(
                handle_output(proc.stdout, stdout_fh),
                handle_output(proc.stderr, stderr_fh),
                proc.wait(),
            )
        finally:
            if stdout_fh:
                stdout_fh.close()
            if stderr_fh and stdout_fh != stderr_fh:
                stderr_fh.close()
        returncode = proc.returncode if proc.returncode is not None else -1
        return (returncode, stdout, stderr)


async def readlines(
    stream: asyncio.StreamReader,
    chunk_size: int = 4096,
    errors: str = "backslashreplace",
):
    """Read lines from a stream."""
    buffer = b""
    while not stream.at_eof():
        chunk = await stream.read(chunk_size)
        if not chunk:
            break
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            yield line.decode(errors=errors)
    if buffer:
        yield buffer.decode(errors=errors)


async def handle_output(stream: asyncio.StreamReader, fh):
    """Process output of lb-prod-run."""
    line_count = 0
    async for line in readlines(stream):
        line_count += 1
        if line_count == 1:
            logger.info("handle_output: first line received from subprocess")
        logger.info(line.rstrip())
        if fh:
            fh.write(line + "\n")
            fh.flush()
    logger.info("handle_output: stream ended after %d lines", line_count)


if __name__ == "__main__":
    main()
