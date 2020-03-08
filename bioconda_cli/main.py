import click
from conda.cli.python_api import run_command
import json
import yaml
import os
import pathlib
import subprocess


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
def env_dump():
    stdout, stderr, retcode = run_command(
        'search',
        '--override-channels',  # Don't use system default channels
        '--channel', 'bioconda',  # Only use bioconda
        '--json'  # We need JSON so we can parse it
    )
    packages = list(json.loads(stdout).keys())
    print(yaml.dump({
        'name': 'all_bioconda',
        'channels': ['bioconda'],
        'dependencies': packages
    }))


@main.command(help='Store all the "--help" outputs in the provided directory')
@click.argument('out', type=click.Path(file_okay=False, dir_okay=True, exists=True))
@click.argument('environment',
                type=click.Path(file_okay=True, dir_okay=False, exists=True))
def list_help(out, environment):
    initial_bin = get_conda_binaries()
    run_command('install', '--file', str(environment))
    final_bin = get_conda_binaries()

    # Output the new binaries installed
    for bin in final_bin - initial_bin:
        proc = subprocess.run(
            [bin, '--help'],
            check=False,
            capture_output=True
        )
        out_file = (out / bin)

        with out_file.with_suffix('.stdout.txt').open('w'):
            out_file.write(proc.stdout)
        with out_file.with_suffix('.stderr.txt').open('w'):
            out_file.write(proc.stderr)

@main.command(help='Convert all the help outputs into CWL')
def acclimatise(dir):
    dir = pathlib.Path(dir)
    for file in dir.iterdir():
        output = file.with_suffix('.cwl')

if __name__ == '__main__':
    main()
