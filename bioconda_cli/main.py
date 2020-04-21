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
    print(msg + "...", end="")
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
@click.option(
    "--last-spec",
    type=click.Path(dir_okay=False),
    help="Path to a previous output from this command, to "
    "ensure we only acclimatise new tool versions",
)
@click.pass_context
def env_dump(ctx, test=False, last_spec=None):
    stdout, stderr, retcode = run_command(
        "search",
        *(
            [
                "search",
                "--override-channels",  # Don't use system default channels
                "--channel",
                "bioconda",  # Only use bioconda
                "--json",  # We need JSON so we can parse it
            ]
            + (["bwa"] if test else [])
        )
    )

    # Get a set of packages at their latest versions in bioconda
    packages = set()
    for key, results in json.loads(stdout).items():
        latest_version = max(results, key=lambda x: version.parse(x["version"]))
        packages.add("{}={}".format(key, latest_version["version"]))

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
    with log_around("Listing conda packages", ctx.obj):
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
            line = pathlib.Path(line)
            with log_around("Exploring {}".format(line), ctx.obj):
                try:
                    cmd = explore_command([str(line)])
                    with (pathlib.Path(out) / line.name).with_suffix(".yml").open(
                        "w"
                    ) as fp:
                        yaml.dump(cmd, fp)
                except Exception as e:
                    print(
                        "Command {} failed with error {} using the output".format(
                            line, e
                        )
                    )


if __name__ == "__main__":
    main()
