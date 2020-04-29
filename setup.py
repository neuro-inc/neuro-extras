import re

from setuptools import find_packages, setup


with open("neuro_extras/__init__.py") as f:
    txt = f.read()
    try:
        version = re.findall(r'^__version__ = "([^"]+)"\r?$', txt, re.M)[0]
    except IndexError:
        raise RuntimeError("Unable to determine version.")


install_requires = [
    "neuromation",
    "yarl",
    "click",
    "toml",
    "pyyaml",
    'dataclasses>=0.5; python_version<"3.7"',
]

setup(
    name="neuro-extras",
    version=version,
    python_requires=">=3.6.0",
    url="https://github.com/neuromation/neuro-extras",
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={"console_scripts": ["neuro-extras=neuro_extras:main"]},
    zip_safe=False,
    include_package_data=True,
)
