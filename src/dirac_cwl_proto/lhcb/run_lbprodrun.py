#!/usr/bin/env python3
"""Wrapper for lb-prod-run that handles CWL inputs and configuration merging."""
import argparse
import asyncio
import json
import logging
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

# Configure logger to use UTC time
logger = logging.getLogger("run_lbprodrun")
logging.Formatter.converter = time.gmtime  # Use UTC for all log timestamps
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


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
            logger.error(f"Success flag is not True: {success.text if success is not None else 'missing'}")
            return False

        # Check step
        step = root.find("step")
        if step is None or step.text != "finalize":
            logger.error(f"Step is not 'finalize': {step.text if step is not None else 'missing'}")
            return False

        # Check input files - all should have status="full"
        input_section = root.find("input")
        if input_section is not None:
            input_files = input_section.findall("file")
            for inp_file in input_files:
                status = inp_file.get("status", "unknown")
                name = inp_file.get("name", "unknown")
                if status != "full":
                    logger.error(f"Input file '{name}' has status '{status}' (expected 'full')")
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
                    logger.error(f"Output file '{name}' has status '{status}' (expected 'full')")
                    return False

        return True

    except ET.ParseError as e:
        logger.error(f"Failed to parse XML summary: {e}")
        return False
    except Exception as e:
        logger.error(f"Error analyzing XML summary: {e}")
        return False


def generate_pool_xml_catalog_from_replica_catalog(
    replica_catalog_path: Path, output_path: Path
) -> None:
    """Generate a pool_xml_catalog.xml from a replica_catalog.json.

    The pool XML catalog format is used by LHCb applications to locate input files.
    This function converts our JSON replica catalog format to the XML format.

    :param replica_catalog_path: Path to replica_catalog.json
    :param output_path: Path where pool_xml_catalog.xml will be written
    """
    from dirac_cwl_proto.job.replica_catalog import ReplicaCatalog

    # Load replica catalog
    catalog = ReplicaCatalog.model_validate_json(replica_catalog_path.read_text())

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

    for lfn, entry in catalog.root.items():
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

    logger.info(f"Generated pool_xml_catalog.xml with {len(catalog.root)} entries")


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


def add_output_files_to_replica_catalog(
    replica_catalog_path: Path,
    output_prefix: str,
    output_types: list[str],
    working_dir: Path = Path("."),
) -> None:
    """Add output files produced by lb-prod-run to the replica catalog.

    lb-prod-run creates output files but doesn't add LFNs to pool XML.
    We need to scan for output files and add them to the replica catalog
    with generated LFNs.

    :param replica_catalog_path: Path to replica_catalog.json
    :param output_prefix: Output file prefix (e.g., "00012345_00006789_1")
    :param output_types: List of output file types (e.g., ["SIM", "DIGI"])
    :param working_dir: Directory to search for output files
    """
    import uuid

    from dirac_cwl_proto.job.replica_catalog import ReplicaCatalog

    # Load existing replica catalog
    if replica_catalog_path.exists():
        catalog = ReplicaCatalog.model_validate_json(replica_catalog_path.read_text())
    else:
        catalog = ReplicaCatalog(root={})

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
                matching_files = list(
                    working_dir.glob(f"{output_prefix}.{ext_variant}")
                )
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
            replica = ReplicaCatalog.CatalogEntry.Replica(
                url=pfn_url, se="DIRAC.Site.Local"
            )
            checksum = ReplicaCatalog.CatalogEntry.Checksum(guid=file_guid)

            entry = ReplicaCatalog.CatalogEntry(
                replicas=[replica], checksum=checksum, size_bytes=file_size
            )

            catalog.root[lfn] = entry
            new_count += 1
            logger.info(f"  Added to catalog: {lfn} -> {local_file.name} ({file_size} bytes)")

    # Write updated catalog
    replica_catalog_path.write_text(catalog.model_dump_json(indent=2))

    if new_count > 0:
        logger.info(f"Added {new_count} output file(s) to replica catalog")
    else:
        logger.info("No output files found to add to replica catalog")


def update_pool_xml_to_absolute_paths(pool_xml_path: Path) -> int:
    """Update all relative PFN paths in pool XML catalog to absolute paths.

    :param pool_xml_path: Path to the pool XML catalog file
    :return: Number of PFNs updated
    """
    if not pool_xml_path.exists():
        logger.warning(f"Pool XML Catalog {pool_xml_path} does not exist.")
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
                    logger.info(f"  Updated: {pfn_name} -> {absolute_path}")
                    updated_count += 1
                else:
                    logger.warning(f"File {pfn_name} not found in current directory")

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

    logger.info(f"Pool XML Catalog updated. {updated_count} PFN(s) converted to absolute paths.")
    return updated_count


def update_replica_catalog_from_pool_xml(
    pool_xml_path: Path, replica_catalog_path: Path
) -> None:
    """Update replica catalog with changes from pool_xml_catalog.xml.

    This function reads the pool XML catalog (which may have been updated by lb-prod-run)
    and propagates any changes back to the replica catalog JSON file. This ensures
    new output files and their metadata are captured in the replica catalog.

    :param pool_xml_path: Path to pool_xml_catalog.xml
    :param replica_catalog_path: Path to replica_catalog.json to update
    """
    from dirac_cwl_proto.job.replica_catalog import ReplicaCatalog

    # Load existing replica catalog if it exists
    if replica_catalog_path.exists():
        catalog = ReplicaCatalog.model_validate_json(replica_catalog_path.read_text())
    else:
        catalog = ReplicaCatalog(root={})

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
                if not pfn_url.startswith(
                    ("file://", "http://", "https://", "root://", "xroot://")
                ):
                    # Assume it's a local absolute path
                    pfn_url = (
                        f"file://{pfn_url}"
                        if pfn_url.startswith("/")
                        else f"file://{Path(pfn_url).resolve()}"
                    )

                    if not lfn:
                        # If LFN is missing, generate one from filename
                        lfn = f"{Path(pfn_url).name}"

                replicas.append(
                    ReplicaCatalog.CatalogEntry.Replica(
                        url=pfn_url, se="DIRAC.Site.Local"
                    )
                )

        if not replicas:
            continue

        # Get file size if the file exists locally
        file_size = None
        for replica in replicas:
            if str(replica.url).startswith("file://"):
                local_path = Path(str(replica.url).replace("file://", ""))
                if local_path.exists():
                    file_size = local_path.stat().st_size
                    break

        # Create or update entry in catalog
        if lfn in catalog.root:
            # Update existing entry
            entry = catalog.root[lfn]
            # Merge replicas (avoid duplicates)
            existing_urls = {str(r.url) for r in entry.replicas}
            for replica in replicas:
                if str(replica.url) not in existing_urls:
                    entry.replicas.append(replica)

            # Update GUID if we have one and it's different
            if guid:
                if entry.checksum is None:
                    entry.checksum = ReplicaCatalog.CatalogEntry.Checksum(guid=guid)
                elif entry.checksum.guid != guid:
                    entry.checksum.guid = guid

            # Update file size if we calculated it
            if file_size is not None and entry.size_bytes != file_size:
                entry.size_bytes = file_size

            updated_count += 1
        else:
            # Create new entry
            checksum = ReplicaCatalog.CatalogEntry.Checksum(guid=guid) if guid else None
            entry = ReplicaCatalog.CatalogEntry(
                replicas=replicas, checksum=checksum, size_bytes=file_size
            )
            catalog.root[lfn] = entry
            new_count += 1

    # Write updated catalog back to file
    replica_catalog_path.write_text(catalog.model_dump_json(indent=2))

    logger.info(
        f"Replica catalog updated: {new_count} new entries, {updated_count} updated entries"
    )
    logger.info(f"Total entries in catalog: {len(catalog.root)}")


def main():
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
        "--replica-catalog",
        help="Replica catalog JSON file (generates pool XML if provided)",
    )
    parser.add_argument("--run-number", type=int, help="Run number")
    parser.add_argument("--first-event-number", type=int, help="First event number")
    parser.add_argument("--number-of-events", type=int, help="Number of events")
    parser.add_argument("--number-of-processors", type=int, help="Number of processors")
    parser.add_argument("--output-prefix", help="Output file prefix")
    parser.add_argument("--event-type", help="Event type ID for Gauss")
    parser.add_argument(
        "--histogram", action="store_true", help="Enable histogram output"
    )

    args = parser.parse_args()

    # Generate pool_xml_catalog from replica_catalog if provided
    if args.replica_catalog:
        replica_catalog_path = Path(args.replica_catalog)
        if replica_catalog_path.exists():
            logger.info(f"Generating pool_xml_catalog.xml from {replica_catalog_path}...")
            try:
                generate_pool_xml_catalog_from_replica_catalog(
                    replica_catalog_path, Path(args.pool_xml_catalog)
                )
            except Exception as e:
                logger.warning(
                    f"Failed to generate pool XML from replica catalog: {e}"
                )
                logger.warning("   Will proceed without pool XML catalog")
        else:
            logger.warning(f"Replica catalog {replica_catalog_path} not found")

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
        config["options"]["files"] = [
            opt.replace("@{eventType}", args.event_type) for opt in options
        ]

    app_name = config["application"]["name"]
    cleaned_appname = app_name.replace("/", "").replace(" ", "")
    
    # Set XML summary file name
    xml_summary_filename = f"summary{cleaned_appname}_{args.output_prefix}.xml"
    config["input"]["xml_summary_file"] = xml_summary_filename

    # Write merged configuration to profConf file
    config_filename = f"prodConf_{cleaned_appname}_{args.output_prefix}.json"
    output_config = Path(config_filename)
    output_config.write_text(json.dumps(config, indent=2))

    # Run lb-prod-run with the merged configuration
    returncode, _, _ = asyncio.run(
        run_lbprodrun(
            application_log=f"{cleaned_appname}_{args.output_prefix}.log",
            prodconf_file=config_filename,
        )
    )

    # Check the summary XML for errors
    xml_summary_path = Path(xml_summary_filename)
    if xml_summary_path.exists():
        logger.info(f"Analyzing XML summary: {xml_summary_filename}")
        is_ok = analyse_xml_summary(xml_summary_path)

        if not is_ok:
            logger.error(f"XML Summary analysis failed for {xml_summary_filename}")
            logger.error("The application reported errors during execution.")
            sys.exit(1)
        else:
            logger.info(f"✅ XML Summary analysis passed for {xml_summary_filename}")
    else:
        logger.warning(f"XML summary file not found: {xml_summary_filename}")

    # Check if any output exists for each filetype
    # {output_prefix}.{filetype}
    for filetype_expected in config["output"].get("types", []):
        expected_filename = f"{args.output_prefix}.{filetype_expected.lower()}"
        if not Path(expected_filename).exists():
            logger.error(
                f"Expected output file not found: {expected_filename} (filetype: {filetype_expected})"
            )
        else:
            logger.info(f"✅ Output file found: {expected_filename}")

    # Update all relative PFN paths in the pool XML catalog to absolute paths
    logger.info("Updating Pool XML Catalog...")
    catalog_path = Path(args.pool_xml_catalog)
    update_pool_xml_to_absolute_paths(catalog_path)

    # Update replica catalog if it was provided
    if args.replica_catalog and catalog_path.exists():
        logger.info("Updating replica catalog from pool XML...")
        try:
            update_replica_catalog_from_pool_xml(
                catalog_path, Path(args.replica_catalog)
            )
        except Exception as e:
            logger.warning(f"Failed to update replica catalog: {e}")

    sys.exit(returncode)


async def run_lbprodrun(
    application_log: str,
    prodconf_file: str,
):
    """Run the application using lb-prod-run"""
    import os

    # Debug: Check if CMAKE_PREFIX_PATH is set
    cmake_prefix = os.environ.get("CMAKE_PREFIX_PATH", "NOT SET")
    analysis_prods_dynamic = os.environ.get("ANALYSIS_PRODUCTIONS_DYNAMIC", "NOT SET")
    logger.info(f"DEBUG: CMAKE_PREFIX_PATH = {cmake_prefix}")
    logger.info(f"DEBUG: ANALYSIS_PRODUCTIONS_DYNAMIC = {analysis_prods_dynamic}")

    command = ["lb-prod-run","--prmon", "--verbose", prodconf_file]

    stdout = ""
    stderr = ""

    proc = await asyncio.create_subprocess_exec(
        *command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout_fh = open(application_log, "a")
    stderr_fh = stdout_fh

    try:
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
    returncode = proc.returncode
    return (returncode, stdout, stderr)


async def readlines(
    stream: asyncio.StreamReader,
    chunk_size: int = 4096,
    errors: str = "backslashreplace",
):
    """Read lines from a stream"""
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
    async for line in readlines(stream):
        if "INFO Evt" in line or "Reading Event record" in line or "lb-run" in line or "Application Manager" in line or "NTuples" in line:
            # These ones will appear in the std.out log too
            logger.info(line.rstrip())
        if fh:
            fh.write(line + "\n")
            fh.flush()


if __name__ == "__main__":
    main()
