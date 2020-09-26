"""
Functions for analysing a batch of files
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

from aclimatise_automation.tool import (
    aclimatise_exe,
    calculate_metadata,
    commands_from_package,
    generate_wrapper,
    reanalyse_tool,
)

from .metadata import BaseCampMeta
from .util import *

logger = getLogger()


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
    listener.stop()


def new_definitions(
    metadata: Path,
    out: Path,
    processes: int = None,
    last_meta: Path = None,
    max_tasks: int = None,
    fork: bool = True,
    wrapper_root: Path = None,
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

    logger.info(
        "There are {} packages in the old metadata and {} in the new. There are {} to process.".format(
            len(new_meta.packages),
            len(old_meta.packages) if last_meta else "?",
            len(to_aclimatise),
        )
    )

    # Iterate each package in the input file
    if fork:
        with Pool(processes, maxtasksperchild=max_tasks) as pool:
            logger.info("Forking into {} processes".format(pool._processes))
            func = partial(
                commands_from_package,
                out=pathlib.Path(out).resolve(),
                logging_queue=queue,
                wrapper_root=wrapper_root,
            )
            pool.map(func, to_aclimatise)
    else:
        for line in to_aclimatise:
            commands_from_package(
                line=line,
                out=pathlib.Path(out).resolve(),
                logging_queue=queue,
                wrapper_root=wrapper_root,
            )
    listener.stop()


def reanalyse(
    dir: Path,
    wrapper_root: Path = None,
    new_meta: Path = None,
    old_meta: Path = None,
    processes: int = None,
    max_tasks: int = None,
    fork: bool = True,
):
    """
    Reanalyses old tool definitions using the latest parser. This requires pre-existing ToolDefinition YAML files
    :param new_meta: The state to upgrade the database to
    :param wrapper_root: If provided, also generate wrappers from the re-analysed tools,
        dump the output into the same folder hierarchy within this directory
    :param dir: The directory containing existing tool definitions
    :param processes: Maximum number of threads to use
    :param old_meta: The previous tool database state
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

    to_reanalyse = dir.rglob("*.yml")

    # Iterate each package in the input file
    if fork:
        with Pool(processes, maxtasksperchild=max_tasks) as pool:
            func = partial(
                reanalyse_tool,
                wrapper_root=wrapper_root,
                logging_queue=queue,
            )
            pool.map(func, to_reanalyse)
    else:
        for file in to_reanalyse:
            reanalyse_tool(
                file,
                wrapper_root=wrapper_root,
                logging_queue=queue,
            )
    listener.stop()
