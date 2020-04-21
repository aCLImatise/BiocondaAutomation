import io
import json
import os
import pathlib
import sys
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from itertools import chain

from conda.cli.python_api import run_command
from conda.exceptions import DryRunExit

import click
from acclimatise import explore_command
from acclimatise.yaml import yaml
from packaging import version


def ctx_print(ctx, msg):
    if ctx.obj["VERBOSE"]:
        print(msg)


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


def get_conda_binaries():
    conda_env = os.environ.get("CONDA_PREFIX")
    if conda_env is None:
        raise Exception("You must be in a conda environment to run this")

    print("Conda env is {}".format(conda_env))
    return set((pathlib.Path(conda_env) / "bin").iterdir())


@click.group()
@click.option("--verbose", is_flag=True)
@click.pass_context
def main(ctx, verbose):
    ctx.ensure_object(dict)
    ctx.obj["VERBOSE"] = verbose


@main.command()
def list_bin():
    print("\n".join([str(x) for x in get_conda_binaries()]))


@main.command(help="Lists all the packages in bioconda")
@click.option(
    "--test", is_flag=True, help="Use a tiny subset of bioconda for testing purposes"
)
@click.pass_context
def list_packages(ctx, test=False):
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

    # The package names are keys to the output dict
    for key in json.loads(stdout).keys():
        print(key)


@main.command(
    help="Produces a file containing all the (system-compatible) versions of all the bioconda packages, "
    "excluding those that haven't changed and don't need upgrading"
)
@click.argument("package_file", type=click.Path(dir_okay=False, exists=True))
@click.option(
    "--last-spec",
    type=click.Path(dir_okay=False),
    help="Path to a previous output from this command, to "
    "ensure we only acclimatise new tool versions",
)
@click.pass_context
def list_versions(ctx, package_file, last_spec=None):
    try:
        stdout, stderr, retcode = run_command(
            "install",
            "--channel",
            "bioconda",
            "--file",
            str(package_file),
            "--json",
            "--dry-run",
            use_exception_handler=True,
        )
    except DryRunExit as e:
        stdout = e.stdout

    # Get a set of packages at their latest compatible versions in bioconda
    packages = set()
    installs = json.loads(stdout)["actions"]["LINK"]
    for package in installs:
        packages.add("{}={}".format(package["name"], package["version"]))

    # The previous spec file basically defines a set of versions *not* to use
    if last_spec is not None:
        with open(last_spec) as fp:
            last_spec_versions = set((line.strip() for line in fp.readlines()))
    else:
        last_spec_versions = set()

    # Subtract the two sets to produce the final result
    sys.stdout.writelines([package + "\n" for package in packages - last_spec_versions])


@main.command(help="Install a list of packages and list the new binaries")
@click.argument("spec", type=click.Path(dir_okay=False))
@click.pass_context
def install(ctx, spec):
    initial_bin = get_conda_binaries()

    with log_around("Installing conda packages", ctx.obj):
        run_command("install", "--channel", "bioconda", "--file", str(spec))

    final_bin = get_conda_binaries()

    for new_bin in final_bin - initial_bin:
        print(str(new_bin) + "\n")


@main.command(help='Store all the "--help" outputs in the provided directory')
@click.argument("bins", type=click.Path(file_okay=True, dir_okay=False, exists=True))
@click.argument("out", type=click.Path(file_okay=False, dir_okay=True, exists=True))
@click.pass_context
def acclimatise(ctx, out, bins):
    with open(bins) as bins_fp:
        # Output the help text to the directory
        for line in bins_fp:
            cmd = pathlib.Path(line.strip())
            with log_around("Exploring {}".format(cmd), ctx.obj):
                try:
                    cmd = explore_command([str(cmd)])
                    with (pathlib.Path(out) / cmd.name).with_suffix(".yml").open(
                        "w"
                    ) as fp:
                        yaml.dump(cmd, fp)
                except Exception as e:
                    print(
                        "Command {} failed with error {} using the output".format(
                            cmd, e
                        )
                    )


if __name__ == "__main__":
    main()
