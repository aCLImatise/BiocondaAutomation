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

from aclimatise import explore_command
from aclimatise.converter.yml import YmlGenerator
from aclimatise.execution.docker import DockerExecutor
from aclimatise.yaml import yaml
from docker.models.containers import Container

logger = getLogger(__name__)


def ctx_print(msg, verbose=True):
    if verbose:
        print(msg, file=sys.stderr)


@contextmanager
def log_around(msg: str, verbose=True, capture=True):
    """
    Wraps a long function invocation with a message like:
    "Running long process... done"
    """
    # Skip this unless we're in verbose mode
    if not verbose:
        yield
        return

    # Store the stdout and stderr to avoid clogging up the logs
    err = io.StringIO()
    out = io.StringIO()
    print(msg + "...", end="", file=sys.stderr)
    with redirect_stderr(err), redirect_stdout(out):
        yield
    print("Done.", file=sys.stderr)

    # Indent the stdout/stderr
    if capture:
        err.seek(0)
        out.seek(0)
        for line in chain(out.readlines(), err.readlines()):
            print("\t" + line, file=sys.stderr, end="")


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
        # Dump a YAML version of the tool
        exhaust(gen.generate_tree(cmd, out_dir))
    except Exception as e:
        handle_exception()
