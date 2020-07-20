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
from acclimatise import Command, CwlGenerator, WdlGenerator, explore_command
from acclimatise.yaml import yaml
from conda.api import Solver
from conda.cli.python_api import run_command
from conda.exceptions import UnsatisfiableError
from packaging.version import parse

# Yes, it's a global: https://stackoverflow.com/a/28268238/2148718
lock = Lock()


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


def get_package_binaries(package, version) -> List[pathlib.Path]:
    """
    Given an already installed package, lists the binaries provided by it
    """
    conda_env = os.environ.get("CONDA_PREFIX")
    if conda_env is None:
        raise Exception("You must be in a conda environment to run this")
    env_path = pathlib.Path(conda_env)

    metadata = list(
        (env_path / "conda-meta").glob("{}-{}*.json".format(package, version))
    )

    if len(metadata) > 1:
        raise Exception("Multiple packages matched the package/version pair")
    if len(metadata) == 0:
        raise Exception("No installed packages matched the package/version pair")

    # The binaries in a given package are listed in the files key of the metadata file
    with metadata[0].open() as fp:
        parsed = json.load(fp)
        # Only return binaries, not just any package file. Their actual location is relative to the prefix
        return [env_path / f for f in parsed["files"] if f.startswith("bin/")]


@contextmanager
def activate_env(env: pathlib.Path):
    env_backup = os.environ.copy()

    # Temporarily set some variables
    os.environ["CONDA_PREFIX"] = str(env)
    os.environ["CONDA_SHLVL"] = "2"
    os.environ["PATH"] = str(env / "bin") + ":" + os.environ["PATH"]
    os.environ["CONDA_PREFIX_1"] = env_backup["CONDA_PREFIX"]

    # Do some action
    yield

    # Then reset those variables
    os.environ.update(env_backup)


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


def install_package(
    versioned_package: str,
    env_dir: pathlib.Path,
    out_dir: pathlib.Path,
    verbose: bool = True,
    exit: bool = False,
):
    """
    Installs a package into an isolated environment
    :param versioned_package:
    :param env_dir:
    :param out_dir:
    :param verbose:
    :param exit:
    :return:
    """
    # Create an empty environment
    run_command(
        "create", "--yes", "--quiet", "--prefix", env_dir,
    )

    with activate_env(pathlib.Path(env_dir)):
        # Generate the query plan concurrently
        solver = Solver(
            str(env_dir),
            ["bioconda", "conda-forge", "r", "main", "free"],
            specs_to_add=[versioned_package],
        )
        try:
            transaction = solver.solve_for_transaction()
        except Exception as e:
            handle_exception(
                e,
                msg="Installing the package {}".format(versioned_package),
                log_path=(out_dir / versioned_package).with_suffix(".error.txt"),
                print=verbose,
                exit=exit,
            )
            return

        # We can't run the installs concurrently, because they used the shared conda packages cache
        with lock:
            transaction.download_and_extract()
            transaction.execute()


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
    exe: pathlib.Path,
    out_dir: pathlib.Path,
    verbose: bool = True,
    exit_on_failure: bool = False,
):
    """
    Given an executable path, acclimatises it, and dumps the results in out_dir
    :param exe:
    :param out_dir:
    :param verbose:
    :param exit_on_failure:
    :return:
    """
    with log_around("Exploring {}".format(exe.name), verbose):
        try:
            # Briefly cd into the temp directory, so we don't fill up the cwd with junk
            cmd = explore_command([exe.name], run_kwargs={"cwd": dir, "check": False})

            # Dump a YAML version of the tool
            with (out_dir / exe.name).with_suffix(".yml").open("w") as out_fp:
                yaml.dump(cmd, out_fp)

        except Exception as e:
            if exit_on_failure:
                raise e
            else:
                handle_exception(
                    e,
                    msg="Acclimatising the command {}".format(exe.name),
                    log_path=(out_dir / exe.name).with_suffix(".error.txt"),
                    print=verbose,
                    exit=exit_on_failure,
                )
