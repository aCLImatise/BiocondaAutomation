from multiprocessing import Pool

from acclimatise import CwlGenerator, WdlGenerator, WrapperGenerator, YmlGenerator

import requests

from .util import *


def list_images(test=False, last_spec=None, verbose=True, filter_r=False):
    images = set()
    result = requests.get(
        "https://api.biocontainers.pro/ga4gh/trs/v2/tools",
        params=dict(toolClass="Docker", limit=10000),
    )

    # The package names are keys to the output dict
    for package in result.json():
        if filter_r and (
            package["name"].startswith("r-")
            or package["name"].startswith("bioconductor-")
        ):
            continue
        latest_version = max(
            package["versions"], key=lambda v: parse(v["meta_version"])
        )
        package_images = sorted(
            requests.get(latest_version["url"]).json()["images"],
            key=lambda image: image["registry_host"] == "registry.hub.docker.com",
        )
        for image in package_images:
            if "image_type" not in image or image["image_type"] == "Docker":
                images.add(image["image_name"])
                break

    # The previous spec file basically defines a set of versions *not* to use
    if last_spec is not None:
        with open(last_spec) as fp:
            last_spec_versions = set((line.strip() for line in fp.readlines()))
    else:
        last_spec_versions = set()

    # Subtract the two sets to produce the final result
    sys.stdout.writelines(
        [package + "\n" for package in sorted(list(images - last_spec_versions))]
    )


def commands_from_package(
    line: str, out: pathlib.Path, verbose=True, exit_on_failure=False
):
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
            install_package(
                versioned_package, dir, out_subdir, verbose, exit_on_failure
            )
            with activate_env(pathlib.Path(dir)):
                new_exes = get_package_binaries(package, version)
                # Acclimatise each new executable
                if len(new_exes) == 0:
                    ctx_print("Package has no executables. Skipping.", verbose)
                for exe in new_exes:
                    acclimatise_exe(
                        exe,
                        out_subdir,
                        verbose=verbose,
                        exit_on_failure=exit_on_failure,
                        run_kwargs={"cwd": dir},
                    )
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
