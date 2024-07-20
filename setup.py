import re

from setuptools import find_packages, setup


with open("apolo_extras/version.py") as f:
    txt = f.read()
    try:
        version = re.findall(r'^__version__ = "([^"]+)"\r?$', txt, re.M)[0]
    except IndexError:
        raise RuntimeError("Unable to determine version.")


setup(
    name="apolo-extras",
    version=version,
    python_requires=">=3.8.0",
    url="https://github.com/neuro-inc/neuro-extras",
    packages=find_packages(),
    install_requires=[
        "apolo-cli>=24.7.1",
        "click>=8.0",
        "toml>=0.10.0",
        "pyyaml>=3.0",
    ],
    entry_points={
        "console_scripts": ["apolo-extras=apolo_extras:main"],
        "apolo_api": ["apolo-extras=apolo_extras:setup_plugin"],
    },
    zip_safe=False,
    include_package_data=True,
)
