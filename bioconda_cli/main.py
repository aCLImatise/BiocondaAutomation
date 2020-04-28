import argparse
import io
import json
import os
import pathlib
import sys
import tempfile
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from functools import partial
from itertools import chain
from logging import ERROR, getLogger
from multiprocessing import Lock, Pool
from typing import List, Tuple

import click
from acclimatise import Command, CwlGenerator, WdlGenerator, explore_command
from acclimatise.yaml import yaml
from conda.api import Solver
from conda.cli.python_api import run_command
from conda.exceptions import UnsatisfiableError
from packaging.version import parse

# Yes, it's a global: https://stackoverflow.com/a/28268238/2148718
lock = Lock()

# This might make conda a bit quieter
getLogger("conda").setLevel(ERROR)


def flush():
    sys.stdout.flush()
    sys.stderr.flush()


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
            print("\t" + line, file=sys.stderr)


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


def main():
    parser = get_parser()
    args = parser.parse_args()
    kwargs = vars(args)
    func = args.func
    kwargs.pop("func")
    func(**kwargs)


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    subparsers = parser.add_subparsers()

    cmd_list = subparsers.add_parser(
        "list-packages", help="Lists all the packages in bioconda, one per line"
    )
    cmd_list.add_argument(
        "--test",
        action="store_true",
        help="Use a tiny subset of bioconda for testing purposes",
    )
    cmd_list.add_argument(
        "--filter-r",
        action="store_true",
        help="Filter out R packages, which don't tend to have CLIs",
    )
    cmd_list.add_argument(
        "--last-spec",
        type=click.Path(dir_okay=False),
        help="Path to a previous output from this command, to "
        "ensure we only acclimatise new tool versions",
    )
    cmd_list.set_defaults(func=list_packages)

    cmd_install = subparsers.add_parser(
        "install", help="Install a list of packages and list the new binaries"
    )
    cmd_install.add_argument(
        "--processes",
        type=int,
        default=None,
        help="Use this many processes instead of all the available CPUs",
    )
    cmd_install.add_argument(
        "packages",
        type=click.Path(dir_okay=False),
        help="A file that has one package with "
        "associated version number, one per line",
    )
    cmd_install.add_argument(
        "out",
        type=click.Path(file_okay=False, dir_okay=True, exists=True),
        help="A directory into which to produce output files",
    )
    cmd_install.set_defaults(func=install)

    return parser


def list_bin(ctx):
    print("\n".join([str(x) for x in get_conda_binaries(ctx)]))


def list_packages(test=False, last_spec=None, verbose=True, filter_r=False):
    with log_around("Listing packages", capture=False, verbose=verbose):
        stdout, stderr, retcode = run_command(
            "search",
            *(
                [
                    "--override-channels",  # Don't use system default channels
                    "--channel",
                    "bioconda",  # Only use bioconda
                    "--json",  # We need JSON so we can parse it
                ]
                + (["bwa"] if test else [])
            )
        )

    packages = set()

    # The package names are keys to the output dict
    for key, versions in json.loads(stdout).items():
        if filter_r and (key.startswith("r-") or key.startswith("bioconductor-")):
            continue
        latest_version = max(versions, key=lambda v: parse(v["version"]))
        packages.add("{}={}".format(key, latest_version["version"]))

    # The previous spec file basically defines a set of versions *not* to use
    if last_spec is not None:
        with open(last_spec) as fp:
            last_spec_versions = set((line.strip() for line in fp.readlines()))
    else:
        last_spec_versions = set()

    # Subtract the two sets to produce the final result
    sys.stdout.writelines([package + "\n" for package in packages - last_spec_versions])


def commands_from_package(line: str, out: pathlib.Path, verbose=True):
    """
    Given a package name, install it in an isolated environment, and acclimatise all package binaries
    """
    versioned_package = line.strip()
    package, version = versioned_package.split("=")

    # Each package should have its own subdirectory
    out_subdir = (out / package) / version
    out_subdir.mkdir(parents=True, exist_ok=True)

    # We have to install and uninstall each package separately because doing it all at once forces Conda to
    # solve an environment with thousands of packages in it, which runs forever (I tried for several days)
    with log_around("Acclimatising {}".format(package), verbose=verbose):
        with tempfile.TemporaryDirectory() as dir:

            # Create an empty environment
            run_command(
                "create", "--yes", "--quiet", "--prefix", dir,
            )

            with activate_env(pathlib.Path(dir)):
                # Generate the query plan concurrently
                solver = Solver(
                    dir,
                    ["bioconda", "conda-forge", "r", "main"],
                    specs_to_add=[versioned_package],
                )
                try:
                    try:
                        transaction = solver.solve_for_transaction()
                    except UnsatisfiableError:
                        ctx_print(
                            "Failed to solve installation for {}. Trying the free channel.".format(
                                versioned_package
                            ),
                            verbose,
                        )
                        # If we can't solve the environment, try adding a new channel
                        solver = Solver(
                            dir,
                            ["bioconda", "conda-forge", "r", "main", "free"],
                            specs_to_add=[versioned_package],
                        )
                        transaction = solver.solve_for_transaction()
                except Exception as e:
                    # If nothing works, just skip this package
                    ctx_print(
                        "Failed to install {}: {}".format(versioned_package, e), verbose
                    )
                    flush()
                    return

                # We can't run the installs concurrently, because they used the shared conda packages cache
                with lock:
                    transaction.download_and_extract()
                    transaction.execute()

                # Acclimatise each new executable
                new_exes = get_package_binaries(package, version)
                if len(new_exes) == 0:
                    ctx_print("Package has no executables. Skipping.", verbose)
                for exe in new_exes:
                    with log_around("Exploring {}".format(exe), verbose):
                        try:
                            # Briefly cd into the temp directory, so we don't fill up the cwd with junk
                            cwd = os.getcwd()
                            os.chdir(dir)
                            cmd = explore_command([exe.name])
                            os.chdir(cwd)

                            # Dump a YAML version of the tool
                            with (out_subdir / exe.name).with_suffix(".yml").open(
                                "w"
                            ) as out_fp:
                                yaml.dump(cmd, out_fp)

                            # Dump a WDL version of the tool
                            wdl = WdlGenerator().generate_wrapper(cmd)
                            (out_subdir / exe.name).with_suffix(".wdl").write_text(wdl)

                            # Dump a CWL version of the tool
                            cwl = CwlGenerator().generate_wrapper(cmd)
                            (out_subdir / exe.name).with_suffix(".cwl").write_text(cwl)

                        except Exception as e:
                            ctx_print(
                                "Command {} failed with error {} using the output".format(
                                    exe, e
                                ),
                                verbose,
                            )
    flush()


def install(packages, out, verbose=False, processes=None):
    # Iterate each package in the input file
    with open(packages) as fp:
        with Pool(processes) as pool:
            lines = fp.readlines()
            func = partial(
                commands_from_package, out=pathlib.Path(out), verbose=verbose
            )
            pool.map(func, lines)


if __name__ == "__main__":
    main()
