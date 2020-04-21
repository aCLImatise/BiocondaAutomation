import io
import json
import os
import pathlib
import sys
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from itertools import chain

from conda.cli.python_api import run_command

import click
from acclimatise import explore_command
from acclimatise.yaml import yaml


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
        return

    # Store the stdout and stderr to avoid clogging up the logs
    err = io.StringIO()
    out = io.StringIO()
    print(msg + "...")
    with redirect_stderr(err), redirect_stdout(out):
        yield
    print("Done.")

    # Indent the stdout/stderr
    for line in chain(out.readlines(), err.readlines()):
        print("\t" + line)


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


@main.command()
@click.option("--test", is_flag=True)
@click.pass_context
def env_dump(ctx, test=False):
    if test:
        packages = [
            # 'samtools',
            "bwa",
            # 'pisces'
        ]
    else:
        stdout, stderr, retcode = run_command(
            "search",
            "--override-channels",  # Don't use system default channels
            "--channel",
            "bioconda",  # Only use bioconda
            "--json",  # We need JSON so we can parse it
        )
        packages = json.loads(stdout).keys()

    sys.stdout.writelines([package + "\n" for package in packages])
    # yaml.dump({
    #     'name': 'all_bioconda',
    #     'channels': ['bioconda'],
    #     'dependencies': packages
    # }, sys.stdout)


@main.command(help='Store all the "--help" outputs in the provided directory')
@click.argument("out", type=click.Path(file_okay=False, dir_okay=True, exists=True))
@click.argument(
    "environment", type=click.Path(file_okay=True, dir_okay=False, exists=True)
)
@click.pass_context
def acclimatise(ctx, out, environment):
    with log_around("Listing conda packages", ctx.obj):
        initial_bin = get_conda_binaries()

    with log_around("Installing conda packages", ctx.obj):
        run_command("install", "--channel", "bioconda", "--file", str(environment))
    final_bin = get_conda_binaries()

    # Output the help text to the directory
    for bin in final_bin - initial_bin:
        with log_around("Exploring {}".format(bin), ctx.obj):
            try:
                cmd = explore_command([str(bin)])
                with (pathlib.Path(out) / bin.name).with_suffix(".yml").open("w") as fp:
                    yaml.dump(cmd, fp)
            except Exception as e:
                print("Command {} failed with error {} using the output".format(bin, e))


if __name__ == "__main__":
    main()
