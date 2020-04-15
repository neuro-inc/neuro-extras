from setuptools import find_packages, setup


install_requires = [
    "neuromation",
    "yarl",
    "click",
    "toml",
    'dataclasses>=0.5; python_version<"3.7"',
]

setup(
    name="neuro-extras",
    version="0.0.1b1",
    python_requires=">=3.6.0",
    url="https://github.com/neuromation/neuro-extras",
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={"console_scripts": ["neuro-extras=neuro_extras:main"]},
    zip_safe=False,
    include_package_data=True,
)
