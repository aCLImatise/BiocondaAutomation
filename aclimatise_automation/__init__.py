import re
import time
from datetime import datetime
from functools import partial
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Manager, Pool, Queue
from pathlib import Path
from typing import Collection, Optional

import docker
from aclimatise import Command, WrapperGenerator, parse_help
from docker.errors import NotFound

from .metadata import BaseCampMeta
from .util import *

logger = getLogger()


def calculate_metadata(
    test=False,
    filter_r=False,
    filter_type: Collection[str] = {"CommandLineTool"},
) -> BaseCampMeta:
    """
    Generates a new metadata file, which is basically a specification for an automation run
    :param test: If we're in test mode
    :param filter_r: If true, filter out R and bioconductor packages
    :param filter_type: A list of toolClasses strings to select, or "none" to disable filtering
    :return:
    """

    # The package names are keys to the output dict
    if test:
        images = ["bwa=0.7.17"]
    else:
        images = latest_biocontainers(filter_r=filter_r, filter_type=filter_type)

    return BaseCampMeta(
        packages=images, aclimatise_version=latest_package_version("aclimatise")
    )


def commands_from_package(
    line: str,
    out: pathlib.Path,
    logging_queue: Queue,
):
    """
    Given a package name, install it in an isolated environment, and aclimatise all package binaries
    """
    versioned_package = line.strip()
    logger = getLogger(versioned_package)
    logger.handlers = []
    logger.addHandler(QueueHandler(logging_queue))

    package, version = versioned_package.split("=")

    # Each package should have its own subdirectory
    try:
        out_subdir = (out / package) / version
        out_subdir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        logger.warning("Directory already exists for {}={}".format(package, version))
        return

    resp = requests.get(
        f"https://api.biocontainers.pro/ga4gh/trs/v2/tools/{package}/versions/{package}-{version}"
    ).json()
    package_images = sorted(
        [
            img
            for img in resp["images"]
            if ("image_type" in img and img["image_type"] == "Docker")
        ],
        key=lambda image: datetime.fromisoformat(image["updated"].rstrip("Z")),
        reverse=True,
    )

    container = None
    try:
        logger.info("aCLImatising {}".format(versioned_package))
        client = docker.from_env()
        for image in package_images:
            formatted_image = re.sub("https?://", "", image["image_name"])
            try:
                container = client.containers.run(
                    image=formatted_image,
                    entrypoint=["sleep", "999999999"],
                    detach=True,
                )
                logger.info("Successfully started")
                break
            except NotFound:
                logger.warning(
                    "Failed to pull from {}, trying next image.".format(formatted_image)
                )
        else:
            logger.error("No images could be pulled")
            return

        # Wait for container to start
        start = time.time()
        while container.status == "starting":
            time.sleep(1)

            if time.time() >= start + 60:
                logger.error("Stopped waiting for container to start after 60 seconds")
                return

        # Anything other than "running" is bad
        if not container.status != "running":
            logger.error(
                "Container {} is not running. It has status {}. Logs show: {}".format(
                    container.id,
                    container.status,
                    container.logs(stderr=True, stdout=True),
                )
            )
            return

        logger.info("Finding binaries")
        new_exes = get_package_binaries(container, package, version)
        logger.info("{} binaries found".format(len(new_exes)))

        # Aclimatise each new executable
        if len(new_exes) == 0:
            logger.error(
                "Package {} has no executables. Skipping.".format(formatted_image)
            )
        for exe in new_exes:
            aclimatise_exe(
                container,
                exe,
                out_dir=out_subdir,
            )

    except Exception as e:
        logger.warning(
            "Exception in process currently processing {}: {}. Cleaning up.".format(
                versioned_package, handle_exception()
            )
        )

    finally:
        if container is not None:
            # Clean up
            container.kill()
            container.remove(force=True)
            container.client.images.remove(container.image.id, force=True)


def generate_wrapper(
    command: pathlib.Path,
    command_dir: pathlib.Path,
    logging_queue: Queue,
    output_dir: Optional[os.PathLike] = None,
):
    """
    Recursively convert all .yml dumped Commands into tool wrappers
    :param command_dir: Root directory to convert from
    :param command: Path to a YAML file to convert
    :param output_dir: If provided, output files in the same directory structure, but in this directory
    """
    logger = getLogger(str(command))
    logger.handlers = []
    logger.addHandler(QueueHandler(logging_queue))

    command = command.resolve()

    logger.info("Converting...")
    with command.open() as fp:
        cmd: Command = yaml.load(fp)

    if output_dir:
        output_path = pathlib.Path(output_dir) / command.parent.relative_to(command_dir)
    else:
        output_path = command.parent

    output_path.mkdir(parents=True, exist_ok=True)

    try:
        generators = [Gen() for Gen in WrapperGenerator.__subclasses__()]
        for cmd in cmd.command_tree():
            if len(cmd.subcommands) > 0:
                # Since we're dumping directly usable tool definitions, it doesn't make sense to dump the parent
                # commands like "samtools" rather than "samtools index", so skip them
                continue

            # Also, if we are dumping, we disconnect each Command from the command tree to simplify the output
            cmd.parent = None
            cmd.subcommands = []

            for gen in generators:
                path = output_path / (cmd.as_filename + gen.suffix)
                gen.save_to_file(cmd, path)
                logger.info(
                    "{} converted to {}".format(" ".join(cmd.command), gen.suffix)
                )
    except Exception as e:
        logger.error(handle_exception())


def wrappers(
    command_dir: os.PathLike,
    output_dir: Optional[os.PathLike] = None,
):
    """
    Recursively convert all .yml dumped Commands into tool wrappers
    :param command_dir: Directory to convert from
    :param output_dir: If provided, output files in the same directory structure, but in this directory
    """
    manager = Manager()
    queue = manager.Queue()
    listener = QueueListener(queue, *logger.handlers)
    listener.start()

    with Pool() as pool:
        packages = pathlib.Path(command_dir).rglob("*.yml")
        func = partial(
            generate_wrapper,
            output_dir=pathlib.Path(output_dir).resolve() if output_dir else None,
            command_dir=pathlib.Path(command_dir).resolve(),
            logging_queue=queue,
        )
        pool.map(func, packages)


def reanalyse_tool(tool: Path, logging_queue: Queue):
    """
    Reanalyses a file, replacing its contents with a new parse
    :param tool: Path to the file to reanalyse
    """
    # Setup a logger for this task
    logger = getLogger(str(tool))
    logger.handlers = []
    logger.addHandler(QueueHandler(logging_queue))

    logger.info("Reanalysing...".format(tool))
    with tool.open() as fp:
        old_cmd: Command = yaml.load(fp)

    if old_cmd.help_text is None or len(old_cmd.help_text) == 0:
        logger.warning("Has no help text to re-analyse")
        return

    if len(old_cmd.subcommands) > 0:
        logger.warning(
            "This tool has subcommands. We can't reanalyse this without re-running it."
        )
        return

    gen = YmlGenerator()
    new_cmd = parse_help(cmd=old_cmd.command, text=old_cmd.help_text)
    gen.save_to_file(new_cmd, tool)


def new_definitions(
    metadata: Path,
    out: Path,
    processes: int = None,
    last_meta: Path = None,
    max_tasks: int = None,
    fork: bool = True,
):
    """
    Generates missing tool definitions, using the given metadata
    :param metadata: The state to upgrade the database to
    :param out: The output directory
    :param processes: Maximum number of threads to use
    :param last_meta: The previous tool database state
    :param max_tasks: Number of tasks before each process is regenerated
    :param fork: If False, don't run in parallel (for debugging)
    """
    manager = Manager()
    queue = manager.Queue()
    listener = QueueListener(queue, *logger.handlers)
    listener.start()

    # Get the latest list of packages
    with metadata.open() as fp:
        new_meta = yaml.load(fp)
    to_aclimatise = set(new_meta.packages)

    if last_meta:
        with last_meta.open() as fp:
            old_meta = yaml.load(fp)
            # We don't have to aclimatise packages we've already done
            to_aclimatise -= set(old_meta.packages)

    # Iterate each package in the input file
    if fork:
        with Pool(processes, maxtasksperchild=max_tasks) as pool:
            func = partial(
                commands_from_package,
                out=pathlib.Path(out).resolve(),
                logging_queue=queue,
            )
            pool.map(func, to_aclimatise)
    else:
        for line in to_aclimatise:
            commands_from_package(
                line=line,
                out=pathlib.Path(out).resolve(),
                logging_queue=queue,
            )


def reanalyse(
    definition_dir: Path,
    new_meta: Path = None,
    old_meta: Path = None,
    processes: int = None,
    max_tasks: int = None,
    fork: bool = True,
):
    """
    Reanalyses old tool definitions using the latest parser. This requires pre-existing ToolDefinition YAML files
    :param new_meta: The state to upgrade the database to
    :param definition_dir: The directory containing existing tool definitions
    :param processes: Maximum number of threads to use
    :param last_meta: The previous tool database state
    :param max_tasks: Number of tasks before each process is regenerated
    :param fork: If False, don't run in parallel (for debugging)
    """
    manager = Manager()
    queue = manager.Queue()
    listener = QueueListener(queue, *logger.handlers)
    listener.start()

    # Get the latest list of packages
    if new_meta is not None and old_meta is not None:
        with new_meta.open() as fp:
            new_meta = yaml.load(fp)

        with old_meta.open() as fp:
            old_meta = yaml.load(fp)

        if not parse(new_meta.aclimatise_version) > parse(old_meta.aclimatise_version):
            # If there hasn't been a new version of the parser, there's no reason to reanalyse
            logger.warning(
                "The previous analysis was done using aclimatise=={}, while the new metadata is for an older or equal version: {}. Skipping.".format(
                    old_meta.aclimatise_version, new_meta.aclimatise_version
                )
            )
            return

    to_reanalyse = definition_dir.rglob("*.yml")

    # Iterate each package in the input file
    if fork:
        with Pool(processes, maxtasksperchild=max_tasks) as pool:
            func = partial(
                reanalyse_tool,
                logging_queue=queue,
            )
            pool.map(func, to_reanalyse)
    else:
        for file in to_reanalyse:
            reanalyse_tool(
                file,
                logging_queue=queue,
            )
