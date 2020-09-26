"""
Functions for analysing single tools (which are called by functions in batch.py)
"""
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

def reanalyse_tool(tool: Path, logging_queue: Queue, wrapper_root: Path = None):
    """
    Reanalyses a file, replacing its contents with a new parse
    :param tool: Path to the file to reanalyse
    :param wrapper_root: If provided, also generate wrappers from the re-analysed tools,
        dump the output into the same folder hierarchy within this directory
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

    if wrapper_root:
        wrapper_from_command(
        cmd=new_cmd,
        command_path=tool,
        command_root=tool.parent.parent.parent.parent,
        wrapper_root=wrapper_root
    )



def commands_from_package(
        line: str,
        out: pathlib.Path,
        logging_queue: Queue,
        wrapper_root: Path = None
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
                wrapper_root=wrapper_root
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

    with command.open() as fp:
        cmd: Command = yaml.load(fp)

    wrapper_from_command(
        cmd=cmd,
        command_path=command,
        command_root=command_dir,
        wrapper_root=Path(output_dir)
    )

