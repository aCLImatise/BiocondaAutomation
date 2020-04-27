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
from multiprocessing import Lock, Pool
from typing import List, Tuple

from conda.cli.python_api import run_command
from conda.exceptions import DryRunExit
from tqdm import tqdm

import click
from acclimatise import Command, explore_command
from acclimatise.yaml import yaml
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


def list_packages(test=False, last_spec=None, verbose=True):
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


def commands_from_package(
    line: str, verbose=True
) -> List[Tuple[Command, pathlib.Path]]:
    """
    Given a package name, install it in an isolated environment, and acclimatise all package binaries
    """
    versioned_package = line.strip()
    package, version = versioned_package.split("=")

    # We have to install and uninstall each package separately because doing it all at once forces Conda to
    # solve an environment with thousands of packages in it, which runs forever (I tried for several days)
    commands = []
    with log_around("Installing {}".format(package), verbose=verbose):
        with tempfile.TemporaryDirectory() as dir:

            # We can't run the installs concurrently, because they used the shared conda packages cache
            with lock:
                run_command(
                    "create",
                    "--yes",
                    "--quiet",
                    "--prefix",
                    dir,
                    "--channel",
                    "bioconda",
                    "--channel",
                    "conda-forge",
                    versioned_package,
                )

            with activate_env(pathlib.Path(dir)):

                # Acclimatise each new executable
                new_exes = get_package_binaries(package, version)
                if len(new_exes) == 0:
                    ctx_print("Packages has no executables. Skipping.", verbose)
                for exe in new_exes:
                    with log_around("Exploring {}".format(exe), verbose):
                        try:
                            cmd = explore_command([str(exe)])
                            commands.append((cmd, exe))
                        except Exception as e:
                            print(
                                "Command {} failed with error {} using the output".format(
                                    exe, e
                                )
                            )
    return commands


def install(packages, out, verbose=False):
    # Iterate each package in the input file
    with open(packages) as fp:
        with Pool() as pool:
            lines = fp.readlines()
            func = partial(commands_from_package, verbose=verbose)
            for commands in tqdm(pool.map(func, lines), total=len(lines)):
                for command, binary in commands:
                    with (pathlib.Path(out) / binary.name).with_suffix(".yml").open(
                        "w"
                    ) as out_fp:
                        yaml.dump(command, out_fp)


if __name__ == "__main__":
    main()
