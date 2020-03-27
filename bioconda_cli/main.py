import click
from conda.cli.python_api import run_command
import json
import yaml
import os
import pathlib
import subprocess


def get_conda_binaries():
    conda_bin = os.environ.get('CONDA_EXE')
    if conda_bin is None:
        raise Exception('You must be in a conda environment to run this')

    return set(pathlib.Path(conda_bin).parent.iterdir())


@click.group()
def main():
    pass


@main.command()
def env_dump():
    stdout, stderr, retcode = run_command('search', 'bwa', '-c', 'bioconda', '--json')
    packages = list(json.loads(stdout).keys())
    print(yaml.dump({
        'name': 'all_bioconda',
        'channels': ['bioconda'],
        'dependencies': packages
    }))


@main.command(help='Store all the "--help" outputs in the provided directory')
@click.argument('out', type=click.Path(file_okay=False, dir_okay=True, exists=True))
@click.argument('environment', type=click.Path(file_okay=True, dir_okay=False, exists=True))
def list_help(out, environment):
    initial_bin = get_conda_binaries()
    run_command('install', '--file', str(environment))
    final_bin = get_conda_binaries()

    # Output the help text to the directory
    for bin in final_bin - initial_bin:
        with (out / bin.name).with_suffix('.txt').open('w') as fp:
            subprocess.run([bin, '--help'], stdout=fp, stderr=fp, check=False)


if __name__ == '__main__':
    main()
