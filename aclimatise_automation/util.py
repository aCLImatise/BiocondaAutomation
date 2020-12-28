"""
Utilities for executing aCLImatise over Bioconda
"""
import io
import json
import os
import pathlib
import sys
import traceback
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from itertools import chain
from logging import getLogger
from multiprocessing import Lock
from typing import Collection, List
from conda.cli.python_api import run_command

import requests
from aclimatise import Command, WrapperGenerator, explore_command
from aclimatise.converter.yml import YmlGenerator
from aclimatise.execution.docker import DockerExecutor
from docker.models.containers import Container
from packaging.version import parse

from aclimatise_automation.metadata import BaseCampMeta
from aclimatise_automation.yml import yaml

logger = getLogger(__name__)


def latest_package_version(package: str) -> str:
    """
    Gets the latest version number of a PyPI package
    """
    req = requests.get("https://pypi.python.org/pypi/{}/json".format(package))
    return req.json()["info"]["version"]


def latest_biocontainers(filter_r: bool, filter_type: List[str]) -> List[str]:
    """
    Returns a list of all the packages in bioconda at their latest versions, represented as {package}={version} strings
    """
    images = set()
    ret = run_command('search', '--channel', 'bioconda', '--json')
    for pname, details in json.loads(ret[0]).items():
        if filter_r and (
                pname.startswith("r-")
                or pname.startswith("bioconductor-")
        ):
            continue

        # Only consider tools of the chosen type
        if len(filter_type) > 0 and details["toolclass"]["name"] not in filter_type:
            continue

        latest_version = max(
            details["versions"], key=lambda v: parse(v["meta_version"])
        )
        images.add("{}={}".format(pname, latest_version["meta_version"]))

    # for package in requests.get(
    #     "https://api.biocontainers.pro/ga4gh/trs/v2/tools",
    #     params=dict(toolClass="Docker", limit=10000),
    # ).json():
    #     if filter_r and (
    #         package["name"].startswith("r-")
    #         or package["name"].startswith("bioconductor-")
    #     ):
    #         continue
    #
    #     # Only consider tools of the chosen type
    #     if len(filter_type) > 0 and package["toolclass"]["name"] not in filter_type:
    #         continue
    #
    #     latest_version = max(
    #         package["versions"], key=lambda v: parse(v["meta_version"])
    #     )
    #     images.add("{}={}".format(package["name"], latest_version["meta_version"]))
    return list(images)


def ctx_print(msg, verbose=True):
    if verbose:
        print(msg, file=sys.stderr)


def get_conda_binaries(verbose):
    conda_env = os.environ.get("CONDA_PREFIX")
    if conda_env is None:
        raise Exception("You must be in a conda environment to run this")

    ctx_print("Conda env is {}".format(conda_env), verbose)
    return set((pathlib.Path(conda_env) / "bin").iterdir())


def get_package_binaries(container: Container, package: str, version: str) -> List[str]:
    """
    Given an already installed package, lists the binaries provided by it
    """
    logger = getLogger(package)
    code, output = container.exec_run(
        "bash -l -c 'cat /usr/local/conda-meta/{}*.json'".format(package),
        demux=True,
        stderr=True,
    )
    stdout, stderr = output

    # The binaries in a given package are listed in the files key of the metadata file
    try:
        parsed = json.loads(stdout)
    except:
        # If the metadata fails to parse, we have to assume there are no binaries
        logger.warning(
            "No metadata file could be identified in the container. Aborting."
        )
        return []

    paths = [pathlib.Path(f) for f in parsed["files"]]

    # Only return binaries, not just any package file. Their actual location is relative to the prefix
    # Filter out files that are within subdirectories inside /bin
    return [
        pathlib.Path(f).name
        for f in paths
        if f.parent.name == "bin" and len(f.parts) == 2
    ]


def list_bin(ctx):
    print("\n".join([str(x) for x in get_conda_binaries(ctx)]))


def handle_exception() -> str:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    return "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))


def exhaust(gen):
    """
    Iterates a generator until it's complete, and discards the items
    """
    for _ in gen:
        pass


def flush():
    sys.stdout.flush()
    sys.stderr.flush()


def aclimatise_exe(
    container: Container,
    exe: str,
    out_dir: pathlib.Path,
    wrapper_root: pathlib.Path = None,
):
    """
    Given an executable path, aclimatises it, and dumps the results in out_dir
    """
    gen = YmlGenerator()
    logger.info("Exploring {}".format(exe))

    try:
        exec = DockerExecutor(container, timeout=10)
        cmd = explore_command(cmd=[exe], executor=exec)
        path = out_dir / (cmd.as_filename + ".yml")
        # Rather than writing out the whole tree, which has redundant information, we instead take the top level command
        # which contains the entire tree, and serialize that
        gen.save_to_file(cmd, path)

        if wrapper_root:
            wrapper_from_command(
                cmd=cmd,
                command_path=path,
                command_root=path.parent.parent.parent,
                wrapper_root=wrapper_root,
            )
    except Exception as e:
        handle_exception()

    logger.info("Successfully written to YAML".format(exe))


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


def wrapper_from_command(
    cmd: Command,
    command_path: pathlib.Path,
    command_root: pathlib.Path,
    wrapper_root: pathlib.Path,
    logger=logger,
):
    """
    Given an already generated command, dump the wrappers
    :param cmd:
    :param command_path:
    :param command_root:
    :param wrapper_root:
    :return:
    """

    output_path = pathlib.Path(wrapper_root) / command_path.parent.relative_to(
        command_root
    )

    logger.info("Outputting wrappers to {}".format(output_path))

    output_path.mkdir(parents=True, exist_ok=True)

    try:
        generators = [Gen() for Gen in WrapperGenerator.__subclasses__()]
        for cmd in cmd.command_tree():
            logger.info("Converting {}".format(cmd.as_filename))
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
