import re

from setuptools import find_packages, setup


with open("neuro_extras/version.py") as f:
    txt = f.read()
    try:
        version = re.findall(r'^__version__ = "([^"]+)"\r?$', txt, re.M)[0]
    except IndexError:
        raise RuntimeError("Unable to determine version.")


setup(
    name="neuro-extras",
    version=version,
    python_requires=">=3.6.0",
    url="https://github.com/neuro-inc/neuro-extras",
    packages=find_packages(),
    install_requires=[
        "neuro-cli>=21.11.2",
        'dataclasses>=0.7; python_version<"3.7"',
        "click>=7.0",
        "toml>=0.10.0",
        "pyyaml>=3.0",
    ],
    entry_points={
        "console_scripts": ["neuro-extras=neuro_extras:main"],
        "neuro_api": ["neuro-extras=neuro_extras:setup_plugin"],
    },
    zip_safe=False,
    include_package_data=True,
)
