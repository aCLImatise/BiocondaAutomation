import re
from datetime import datetime
from functools import partial
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Pool, Queue, Manager
from typing import Collection, Optional

from acclimatise import WrapperGenerator
from packaging.version import parse

import docker
import requests
from docker.errors import NotFound

from .util import *

logger = getLogger()


def list_images(
    test=False,
    last_spec=None,
    verbose=True,
    filter_r=False,
    filter_type: Collection[str] = {"CommandLineTool"},
):
    """
    :param test:
    :param last_spec:
    :param verbose:
    :param filter_r: If true, filter out R and bioconductor packages
    :param filter_type: A list of toolClasses strings to select, or "none" to disable filtering
    :return:
    """

    # The package names are keys to the output dict
    if test:
        images = {"bwa=0.7.17"}
    else:
        images = set()
        for package in requests.get(
            "https://api.biocontainers.pro/ga4gh/trs/v2/tools",
            params=dict(toolClass="Docker", limit=10000),
        ).json():
            if filter_r and (
                package["name"].startswith("r-")
                or package["name"].startswith("bioconductor-")
            ):
                continue

            # Only consider tools of the chosen type
            if len(filter_type) > 0 and package["toolclass"]["name"] not in filter_type:
                continue

            latest_version = max(
                package["versions"], key=lambda v: parse(v["meta_version"])
            )
            images.add("{}={}".format(package["name"], latest_version["meta_version"]))

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
    line: str,
    out: pathlib.Path,
    logging_queue: Queue,
    verbose=True,
    exit_on_failure=False,
):
    """
    Given a package name, install it in an isolated environment, and acclimatise all package binaries
    """
    logger = getLogger(line)
    logger.handlers = []
    logger.addHandler(QueueHandler(logging_queue))

    versioned_package = line.strip()
    package, version = versioned_package.split("=")

    # Each package should have its own subdirectory
    try:
        out_subdir = (out / package) / version
        out_subdir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        logger.warning("Directory already exists for {}={}".format(package, version))
        return

    resp = requests.get(
        f"https://api.biocontainers.pro/ga4gh/trs/v2/tools/{package}/versions/{package}-{version}"
    ).json()
    package_images = sorted(
        [
            img
            for img in resp["images"]
            if ("image_type" in img and img["image_type"] == "Docker")
        ],
        key=lambda image: datetime.fromisoformat(image["updated"].rstrip("Z")),
        reverse=True,
    )

    container = None
    try:
        logger.info("aCLImatising {}".format(line))
        client = docker.from_env()
        for image in package_images:
            formatted_image = re.sub("https?://", "", image["image_name"])
            try:
                container = client.containers.run(
                    image=formatted_image,
                    entrypoint=["sleep", "999999999"],
                    detach=True,
                )
                logger.info("Successfully started {}".format(formatted_image))
                break
            except NotFound:
                logger.warning(
                    "Failed to pull from {}, trying next image.".format(formatted_image)
                )
        else:
            logger.error("No images could be pulled for tool {}.".format(line))
            return

        # Wait for container to start
        while container.status == 'starting':
            time.sleep(1)

        # Anything other than "running" is bad
        if not container.status != "running":
            logger.error(
                "Container {} is not running. It has status {}. Logs show: {}".format(
                    container.id,
                    container.status,
                    container.logs(stderr=True, stdout=True)
                )
            )
            return

        new_exes = get_package_binaries(container, package, version)

        # Acclimatise each new executable
        if len(new_exes) == 0:
            logger.error("Package {} has no executables. Skipping.".format(formatted_image))
        for exe in new_exes:
            acclimatise_exe(
                container, exe, out_dir=out_subdir, verbose=verbose,
            )

    except Exception as e:
        logger.warning(
            "Exception in process currently processing {}: {}. Cleaning up.".format(
                line, handle_exception()
            )
        )

    finally:
        if container is not None:
            # Clean up
            container.kill()
            container.remove(force=True)
            container.client.images.remove(container.image.id, force=True)


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
            logger.error(handle_exception())


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
    packages,
    out,
    verbose=False,
    processes=None,
    exit_on_failure=False,
    max_tasks=None,
    fork=True,
):
    manager = Manager()
    queue = manager.Queue()
    listener = QueueListener(queue, *logger.handlers)
    listener.start()

    # Iterate each package in the input file
    with open(packages) as fp:
        lines = fp.readlines()
        if fork:
            with Pool(processes, maxtasksperchild=max_tasks) as pool:
                func = partial(
                    commands_from_package,
                    out=pathlib.Path(out).resolve(),
                    verbose=verbose,
                    exit_on_failure=exit_on_failure,
                    logging_queue=queue,
                )
                pool.map(func, lines)
        else:
            for line in lines:
                commands_from_package(
                    line=line,
                    out=pathlib.Path(out).resolve(),
                    verbose=verbose,
                    exit_on_failure=exit_on_failure,
                    logging_queue=queue,
                )
