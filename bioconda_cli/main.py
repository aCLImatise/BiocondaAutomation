"""
CLI for executing aCLImatise over Bioconda
"""
import subprocess
from multiprocessing import Pool

from acclimatise import CwlGenerator, WdlGenerator, WrapperGenerator, YmlGenerator

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
    cmd_list.set_defaults(func=list_packages)

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

        py = False
        for d in latest_version["depends"]:
            if "python" in d:
                py = True
                break
        if not py:
            continue

        packages.add("{}={}".format(key, latest_version["version"]))

    # The previous spec file basically defines a set of versions *not* to use
    if last_spec is not None:
        with open(last_spec) as fp:
            last_spec_versions = set((line.strip() for line in fp.readlines()))
    else:
        last_spec_versions = set()

    # Subtract the two sets to produce the final result
    sys.stdout.writelines(
        [package + "\n" for package in sorted(list(packages - last_spec_versions))]
    )


def commands_from_package(
    line: str, out: pathlib.Path, verbose=True, exit_on_failure=False
):
    """
    Given a package name, install it in an isolated environment, and acclimatise all package binaries
    """
    versioned_package = line.strip()
    package, version = versioned_package.split("=")
    print("commands_from_package %s" % versioned_package)

    # Each package should have its own subdirectory
    out_subdir = (out / package) / version
    out_subdir.mkdir(parents=True, exist_ok=True)

    # We have to install and uninstall each package separately because doing it all at once forces Conda to
    # solve an environment with thousands of packages in it, which runs forever (I tried for several days)
    with log_around("Acclimatising {}".format(package), verbose=verbose):
        dir = "/tmp/bioconda_cli/%s" % versioned_package
        if 1:
            # with tempfile.TemporaryDirectory() as dir:
            install_package(
                versioned_package, dir, out_subdir, verbose, exit_on_failure
            )
            with activate_env(pathlib.Path(dir)):
                new_exes = get_package_binaries(package, version)
                # Acclimatise each new executable
            #                 if len(new_exes) == 0:
            #                     ctx_print("Package has no executables. Skipping.", verbose)
            #                 for exe in new_exes:
            #                     print("proecessing exe %s" % exe)
            #                     acclimatise_exe(
            #                         exe,
            #                         out_subdir,
            #                         verbose=verbose,
            #                         exit_on_failure=exit_on_failure,
            #                         run_kwargs={"cwd": dir},
            #                     )
            for exe in new_exes:
                cmd = """. /home/berntm/miniconda3/etc/profile.d/conda.sh &&
conda activate %s &&
PYTHONPATH="$(argparse2tool)" %s --generate_galaxy_xml
""" % (
                    dir,
                    exe,
                )
                proc = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True)
                if proc.returncode:
                    print("failed for %s %s" % (exe, proc))
                    continue
                ofn = pathlib.Path("%s-%s.xml" % (package, os.path.basename(exe)))
                with open(out_subdir / ofn, "wb") as ofh:
                    ofh.write(proc.stdout)

    flush()


def generate_wrapper(
    command: pathlib.Path,
    command_dir: pathlib.Path,
    output_dir: Optional[os.PathLike] = None,
    verbose: bool = True,
):
    """
    Recursively convert all .yml dumped Commands into tool wrappers
    :param command_dir: Root directory to convert from
    :param command: Path to a YAML file to convert
    :param output_dir: If provided, output files in the same directory structure, but in this directory
    """
    command = command.resolve()

    with log_around("Converting {}".format(command), verbose):
        with command.open() as fp:
            cmd = yaml.load(fp)

        if output_dir:
            output_path = pathlib.Path(output_dir) / command.parent.relative_to(
                command_dir
            )
        else:
            output_path = command.parent

        output_path.mkdir(parents=True, exist_ok=True)

        try:
            for subclass in WrapperGenerator.__subclasses__():
                gen = subclass()
                exhaust(gen.generate_tree(cmd, output_path))
        except Exception as e:
            handle_exception(
                e,
                msg="Converting the command {}".format(command),
                log_path=command.with_suffix(".error"),
                print=verbose,
            )


def wrappers(
    command_dir: os.PathLike,
    output_dir: Optional[os.PathLike] = None,
    verbose: bool = True,
):
    """
    Recursively convert all .yml dumped Commands into tool wrappers
    :param command_dir: Directory to convert from
    :param output_dir: If provided, output files in the same directory structure, but in this directory
    """
    with Pool() as pool:
        packages = pathlib.Path(command_dir).rglob("*.yml")
        func = partial(
            generate_wrapper,
            output_dir=pathlib.Path(output_dir).resolve() if output_dir else None,
            verbose=verbose,
            command_dir=pathlib.Path(command_dir).resolve(),
        )
        pool.map(func, packages)


def install(
    packages, out, verbose=False, processes=None, exit_on_failure=False, max_tasks=None
):
    # Iterate each package in the input file
    with open(packages) as fp:
        with Pool(processes, maxtasksperchild=max_tasks) as pool:
            lines = fp.readlines()
            func = partial(
                commands_from_package,
                out=pathlib.Path(out).resolve(),
                verbose=verbose,
                exit_on_failure=exit_on_failure,
            )
            pool.map(func, lines)


if __name__ == "__main__":
    main()
