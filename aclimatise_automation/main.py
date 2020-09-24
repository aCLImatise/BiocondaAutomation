"""
CLI for executing aCLImatise over Bioconda
"""

import argparse
import sys
from logging import ERROR, FileHandler, getLogger
from pathlib import Path

import click

from aclimatise_automation import (
    calculate_metadata,
    new_definitions,
    reanalyse,
    wrappers,
)
from aclimatise_automation.yml import yaml

# This might make conda a bit quieter
getLogger("conda").setLevel(ERROR)


class PathPath(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""

    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


def main():
    parser = get_parser()
    args = parser.parse_args()
    kwargs = vars(args)

    # Write to a log file if provided
    logger = getLogger()
    log_file = kwargs.pop("log_file", None)
    if log_file is not None:
        logger.addHandler(FileHandler(log_file))

    func = args.func
    kwargs.pop("func")
    func(**kwargs)


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", type=Path)
    subparsers = parser.add_subparsers()

    cmd_list = subparsers.add_parser(
        "metadata", help="Lists all the packages in bioconda, one per line"
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
        "--filter-type",
        help="A toolClasses string to select, or False to disable filtering",
        action="append",
        default=["CommandLineTool"],
        choices=["CommandLineTool", "Workflow", "CommandLineMultiTool", "Service"],
    )
    cmd_list.set_defaults(
        func=lambda *args, **kwargs: yaml.dump(
            calculate_metadata(*args, **kwargs), stream=sys.stdout
        )
    )

    cmd_install = subparsers.add_parser(
        "install", help="Install a list of packages and list the new binaries"
    )
    cmd_install.add_argument(
        "--last-meta",
        type=PathPath(dir_okay=False),
        help="Path to a previous output from the meta command, to ensure we only aclimatise new tool versions",
    )
    cmd_install.add_argument(
        "--processes",
        "-p",
        type=int,
        default=None,
        help="Use this many processes instead of all the available CPUs",
    )
    cmd_install.add_argument(
        "--debug",
        action="store_false",
        dest="fork",
        help="Don't fork using multiprocessing, allowing for PDB debugging",
    )
    cmd_install.add_argument(
        "--max-tasks",
        "-m",
        type=int,
        default=None,
        help="The number of packages each process will analyse before it is replaced with a fresh worker process",
    )
    cmd_install.add_argument(
        "metadata",
        type=PathPath(dir_okay=False),
        help="A file that has one package with "
        "associated version number, one per line",
    )
    cmd_install.add_argument(
        "out",
        type=PathPath(file_okay=False, dir_okay=True, exists=True),
        help="A directory into which to produce output files",
    )
    cmd_install.set_defaults(func=new_definitions)

    cmd_reanalyse = subparsers.add_parser(
        "reanalyse",
        help="Re-analyse all tool definitions in a directory using the latest parser. Doesn't look at parent commands since these cannot be re-analysed without rerunning the command.",
    )
    cmd_reanalyse.add_argument(
        "dir",
        type=PathPath(file_okay=False, dir_okay=True, exists=True),
        help="The directory to re-analyse",
    )
    cmd_reanalyse.add_argument(
        "--old-meta",
        type=PathPath(file_okay=True, dir_okay=False, exists=True),
        help="The metadata file used in the last analysis",
    )
    cmd_reanalyse.add_argument(
        "--new-meta",
        type=PathPath(file_okay=True, dir_okay=False, exists=True),
        help="An up-to-date metadata file",
    )
    cmd_reanalyse.add_argument(
        "--processes",
        "-p",
        type=int,
        default=None,
        help="Use this many processes instead of all the available CPUs",
    )
    cmd_reanalyse.add_argument(
        "--debug",
        action="store_false",
        dest="fork",
        help="Don't fork using multiprocessing, allowing for PDB debugging",
    )
    cmd_reanalyse.add_argument(
        "--max-tasks",
        "-m",
        type=int,
        default=None,
        help="The number of packages each process will analyse before it is replaced with a fresh worker process",
    )
    cmd_reanalyse.set_defaults(func=reanalyse)

    cmd_wrappers = subparsers.add_parser(
        "wrappers",
        help="Recursively convert all .yml dumped Commands into tool wrappers",
    )
    cmd_wrappers.add_argument(
        "command_dir", type=PathPath(dir_okay=True, file_okay=False, exists=True)
    )
    cmd_wrappers.add_argument(
        "--output-dir",
        "-o",
        type=PathPath(dir_okay=True, file_okay=False, exists=True),
    )
    cmd_wrappers.set_defaults(func=wrappers)

    return parser


if __name__ == "__main__":
    main()
