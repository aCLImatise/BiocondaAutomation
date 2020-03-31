import click
from conda.cli.python_api import run_command
import json
import os
import pathlib
from acclimatise import best_cmd
from acclimatise.yaml import yaml
import sys


def get_conda_binaries():
    conda_env = os.environ.get('CONDA_PREFIX')
    if conda_env is None:
        raise Exception('You must be in a conda environment to run this')

    return set((pathlib.Path(conda_env) / 'bin').iterdir())


@click.group()
def main():
    pass


@main.command()
def list_bin():
    print('\n'.join([str(x) for x in get_conda_binaries()]))


@main.command()
@click.option('--test', is_flag=True)
def env_dump(test=False):
    if test:
        packages = [
            'samtools',
            'bwa',
            'pisces'
        ]
    else:
        stdout, stderr, retcode = run_command(
            'search',
            '--override-channels',  # Don't use system default channels
            '--channel', 'bioconda',  # Only use bioconda
            '--json'  # We need JSON so we can parse it
        )
        packages = json.loads(stdout).keys()

    sys.stdout.writelines([package + '\n' for package in packages])
    # yaml.dump({
    #     'name': 'all_bioconda',
    #     'channels': ['bioconda'],
    #     'dependencies': packages
    # }, sys.stdout)


@main.command(help='Store all the "--help" outputs in the provided directory')
@click.argument('out', type=click.Path(file_okay=False, dir_okay=True, exists=True))
@click.argument('environment',
                type=click.Path(file_okay=True, dir_okay=False, exists=True))
def acclimatise(out, environment):
    initial_bin = get_conda_binaries()
    run_command(
        'install',
        '--channel', 'bioconda',
        '--file', str(environment)
    )
    final_bin = get_conda_binaries()

    # Output the help text to the directory
    for bin in final_bin - initial_bin:
        cmd = best_cmd([str(bin)])
        with (out / bin.name).with_suffix('.yml').open('w') as fp:
            yaml.dump(cmd, fp)


if __name__ == '__main__':
    main()
