import io
import json
import os
import pathlib
import sys
import tempfile
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from itertools import chain

from conda.cli.python_api import run_command
from conda.exceptions import DryRunExit

import click
from acclimatise import explore_command
from acclimatise.yaml import yaml
from packaging.version import parse


def ctx_print(ctx, msg):
    if ctx.obj["VERBOSE"]:
        print(msg, file=sys.stderr)


@contextmanager
def log_around(msg: str, ctx: dict = {}):
    """
    Wraps a long function invocation with a message like:
    "Running long process... done"
    """
    # Skip this unless we're in verbose mode
    if not ctx.get("VERBOSE"):
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
    for line in chain(out.readlines(), err.readlines()):
        print("\t" + line, file=sys.stderr)


def get_conda_binaries(ctx):
    conda_env = os.environ.get("CONDA_PREFIX")
    if conda_env is None:
        raise Exception("You must be in a conda environment to run this")

    ctx_print(ctx, "Conda env is {}".format(conda_env))
    return set((pathlib.Path(conda_env) / "bin").iterdir())


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


@click.group()
@click.option("--verbose", is_flag=True)
@click.pass_context
def main(ctx, verbose):
    ctx.ensure_object(dict)
    ctx.obj["VERBOSE"] = verbose


@main.command()
@click.pass_context
def list_bin(ctx):
    print("\n".join([str(x) for x in get_conda_binaries(ctx)]))


@main.command(help="Lists all the packages in bioconda, one per line")
@click.option(
    "--test", is_flag=True, help="Use a tiny subset of bioconda for testing purposes"
)
# @click.option('--versions', is_flag=True, help='Include package versions in output')
@click.option(
    "--last-spec",
    type=click.Path(dir_okay=False),
    help="Path to a previous output from this command, to "
    "ensure we only acclimatise new tool versions",
)
@click.pass_context
def list_packages(ctx, test=False, last_spec=None):
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


# @main.command(
#     help="Produces a file containing all the (system-compatible) versions of all the bioconda packages, "
#     "excluding those that haven't changed and don't need upgrading"
# )
# @click.argument("package_file", type=click.Path(dir_okay=False, exists=True))
# @click.option(
#     "--last-spec",
#     type=click.Path(dir_okay=False),
#     help="Path to a previous output from this command, to "
#     "ensure we only acclimatise new tool versions",
# )
# @click.pass_context
# def list_versions(ctx, package_file, last_spec=None):
#     try:
#         stdout, stderr, retcode = run_command(
#             "install",
#             "--channel",
#             "bioconda",
#             "--file",
#             str(package_file),
#             "--json",
#             "--dry-run",
#             use_exception_handler=True,
#         )
#     except DryRunExit as e:
#         stdout = e.stdout
#
#     # Get a set of packages at their latest compatible versions in bioconda
#     packages = set()
#     installs = json.loads(stdout)["actions"]["LINK"]
#     for package in installs:
#         packages.add("{}={}".format(package["name"], package["version"]))
#
#     # The previous spec file basically defines a set of versions *not* to use
#     if last_spec is not None:
#         with open(last_spec) as fp:
#             last_spec_versions = set((line.strip() for line in fp.readlines()))
#     else:
#         last_spec_versions = set()
#
#     # Subtract the two sets to produce the final result
#     sys.stdout.writelines([package + "\n" for package in packages - last_spec_versions])


@main.command(help="Install a list of packages and list the new binaries")
# A file which contains one package per line
@click.argument("packages", type=click.Path(dir_okay=False))
@click.argument("out", type=click.Path(file_okay=False, dir_okay=True, exists=True))
@click.pass_context
def install(ctx, packages, out):
    # Ignore all the default conda packages
    initial_bin = get_conda_binaries(ctx)

    # Iterate each package in the input file
    with open(packages) as fp:
        for line in fp:
            versioned_package = line.strip()
            package, version = versioned_package.split("=")

            # We have to install and uninstall each package separately because doing it all at once forces Conda to
            # solve an environment with thousands of packages in it, which runs forever (I tried for several days)
            with log_around("Installing {}".format(package), ctx.obj):
                with tempfile.TemporaryDirectory() as dir:
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
                        new_bin = get_conda_binaries(ctx)

                        # Acclimatise each new executable
                        new_exes = new_bin - initial_bin
                        if len(new_exes) == 0:
                            ctx_print(ctx, "Packages has no executables. Skipping.")
                        for exe in new_exes:
                            with log_around("Exploring {}".format(exe), ctx.obj):
                                try:
                                    cmd = explore_command([str(exe)])
                                    with (pathlib.Path(out) / exe.name).with_suffix(
                                        ".yml"
                                    ).open("w") as out_fp:
                                        yaml.dump(cmd, out_fp)
                                except Exception as e:
                                    print(
                                        "Command {} failed with error {} using the output".format(
                                            exe, e
                                        )
                                    )


# @main.command(help='Store all the "--help" outputs in the provided directory')
# @click.argument("bins", type=click.Path(file_okay=True, dir_okay=False, exists=True))
# @click.argument("out", type=click.Path(file_okay=False, dir_okay=True, exists=True))
# @click.pass_context
# def acclimatise(ctx, out, bins):
#     with open(bins) as bins_fp:
#         # Output the help text to the directory
#         for line in bins_fp:
#             exe = pathlib.Path(line.strip())
#             with log_around("Exploring {}".format(exe), ctx.obj):
#                 try:
#                     cmd = explore_command([str(exe)])
#                     with (pathlib.Path(out) / exe.name).with_suffix(".yml").open(
#                         "w"
#                     ) as fp:
#                         yaml.dump(cmd, fp)
#                 except Exception as e:
#                     print(
#                         "Command {} failed with error {} using the output".format(
#                             exe, e
#                         )
#                     )


if __name__ == "__main__":
    main()
