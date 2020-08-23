"""
CLI for executing aCLImatise over Bioconda
"""

import argparse
from logging import ERROR

import click

from bioconda_cli import *
from bioconda_cli.util import *

# This might make conda a bit quieter
getLogger("conda").setLevel(ERROR)


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
    cmd_list.set_defaults(func=list_images)

    cmd_install = subparsers.add_parser(
        "install", help="Install a list of packages and list the new binaries"
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
        test="fork",
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
        "--exit-on-failure",
        "-x",
        action="store_true",
        help="Exit the entire process if any package fails",
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

    cmd_wrappers = subparsers.add_parser(
        "wrappers",
        help="Recursively convert all .yml dumped Commands into tool wrappers",
    )
    cmd_wrappers.add_argument(
        "command_dir", type=click.Path(dir_okay=True, file_okay=False, exists=True)
    )
    cmd_wrappers.add_argument(
        "--output-dir",
        "-o",
        type=click.Path(dir_okay=True, file_okay=False, exists=True),
    )
    cmd_wrappers.set_defaults(func=wrappers)

    return parser


if __name__ == "__main__":
    main()
