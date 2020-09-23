from setuptools import find_packages, setup

setup(
    name="aCLImatise-automation",
    packages=find_packages(),
    version="0.0.1",
    install_requires=[
        "click",
        "aclimatise>=2.0.0",
        "ruamel.yaml==0.16.5",
        "packaging",
        "tqdm",
        "requests",
        "docker",
    ],
    extras_require={"dev": ["pytest", "pre-commit"],},
    entry_points={
        "console_scripts": ["aclimatise-automation = aclimatise_automation.main:main",],
    },
)
