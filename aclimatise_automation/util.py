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
from typing import List

from packaging.version import parse

import requests
from aclimatise import explore_command
from aclimatise.converter.yml import YmlGenerator
from aclimatise.execution.docker import DockerExecutor
from aclimatise_automation.yml import yaml
from docker.models.containers import Container
from git import Repo

logger = getLogger(__name__)


def last_git_update(path: pathlib.Path) -> int:
    """
    Returns the last date of update as a unix timestamp, according to a git repo
    """
    repo = Repo(path.parent, search_parent_directories=True)
    commit = next(repo.iter_commits(paths=path))
    return commit.authored_date


def latest_package_version(package: str) -> str:
    """
    Gets the latest version number of a PyPI package
    """
    req = requests.get("https://pypi.python.org/pypi/{}/json".format(package))
    return req.json()["info"]["version"]


def latest_biocontainers(filter_r: bool, filter_type: List[str]) -> List[str]:
    images = set()
    for package in requests.get(
        "https://api.biocontainers.pro/ga4gh/trs/v2/tools",
        params=dict(toolClass="Docker", limit=10000),
    ).json():
        if filter_r and (
            package["name"].startswith("r-")
            or package["name"].startswith("bioconductor-")
        ):
            continue

        # Only consider tools of the chosen type
        if len(filter_type) > 0 and package["toolclass"]["name"] not in filter_type:
            continue

        latest_version = max(
            package["versions"], key=lambda v: parse(v["meta_version"])
        )
        images.add("{}={}".format(package["name"], latest_version["meta_version"]))
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
    code, output = container.exec_run(
        "bash -l -c 'cat /usr/local/conda-meta/{}*.json'".format(package),
        demux=True,
        stderr=True,
    )
    stdout, stderr = output

    # The binaries in a given package are listed in the files key of the metadata file
    parsed = json.loads(stdout)
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
    except Exception as e:
        handle_exception()

    logger.info("Successfully written to YAML".format(exe))
