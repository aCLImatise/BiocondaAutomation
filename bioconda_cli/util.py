"""
Utilities for executing aCLImatise over Bioconda
"""
import argparse
import io
import json
import os
import pathlib
import sys
import tempfile
import traceback
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from functools import partial
from itertools import chain
from logging import ERROR, getLogger
from multiprocessing import Lock, Pool
from typing import List, Optional, Tuple

import click
import docker
from acclimatise import Command, CwlGenerator, WdlGenerator, explore_command
from acclimatise.yaml import yaml
from docker.models.containers import Container
from packaging.version import parse

# Yes, it's a global: https://stackoverflow.com/a/28268238/2148718
lock = Lock()

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


def get_package_binaries(
    container: Container, package: str, version: str
) -> List[pathlib.Path]:
    """
    Given an already installed package, lists the binaries provided by it
    """
    _, root = container.exec_run("bash -l -c 'printenv CONDA_ROOT'")
    code, output = container.exec_run(
        ["{}/conda-meta/{}-{}*.json".format(root, package, version)],
        demux=True,
        stderr=True,
    )
    stdout, stderr = output

    # The binaries in a given package are listed in the files key of the metadata file
    parsed = json.loads(stdout)
    # Only return binaries, not just any package file. Their actual location is relative to the prefix
    return [pathlib.Path(root) / f for f in parsed["files"] if f.startswith("bin/")]


def list_bin(ctx):
    print("\n".join([str(x) for x in get_conda_binaries(ctx)]))


def handle_exception(
    exception,
    msg,
    log_path: pathlib.Path = None,
    print: bool = True,
    exit: bool = False,
):
    if exit:
        raise exception
    else:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        message = "{} failed with error {}".format(
            msg,
            "".join(traceback.format_exception(exc_type, exc_value, exc_traceback)),
        )
        # Log the error to a file, and also stderr
        log_path.write_text(message)
        ctx_print(message, print)


def exhaust(gen):
    """
    Iterates a generator until it's complete, and discards the items
    """
    for _ in gen:
        pass


def flush():
    sys.stdout.flush()
    sys.stderr.flush()


def acclimatise_exe(
    container: Container,
    exe: pathlib.Path,
    out_dir: pathlib.Path,
    verbose: bool = True,
):
    """
    Given an executable path, acclimatises it, and dumps the results in out_dir
    """

    with log_around("Exploring {}".format(exe.name), verbose):
        code, output = container.exec_run(
            ["pip", "install", "acclimatise"], stderr=True
        )
        if code != 0:
            logger.error(
                "Failed to install aCLImatise for container {}.".format(exe.name)
            )

        code, output = container.exec_run(
            ["acclimatise", "explore", exe.name, "--out-dir", str(out_dir)], stderr=True
        )
        if code != 0:
            logger.error(
                "Failed to aCLImatise {}. Failed with stderr: \n{}".format(
                    exe.name, output
                )
            )
