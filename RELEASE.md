# Versioning

The project uses release versioning a format `YY.MM.DD`. For instance, when releasing a version on Sep 15, 2021, it would be versionsed as `21.9.15` (no zero before `9`). Note: you'll be setting version in format `vYY.MM.DD`, prefix `v` will be cut automatically.


# Release process

*Note*: Start with part 1 if the docker image should be updated. Otherwise - go directly to part 2.

Suppose, today is October 14, 2020, and we want to update both neuro-extras pip package and docker image.

0. Make sure all tests are green in `master` branch.

## Part 1: alpha-release

1. Bump alpha version in the code.
    - `git checkout master`;
    - update `neuro_extras/__init__.py`: `__version__ = "v20.10.15a1"` (note postfix `a1`);
    - `git commit -m "Update __init__.py version to v20.10.15a1"` and `push` directly to `origin/master`;
    - wait for green build in [Actions](https://github.com/neuro-inc/neuro-extras/actions);

2. Submit a pre-release on GitHub.
    - go to [Releases](https://github.com/neuro-inc/neuro-extras/releases/), `Draft a new release`, tag: `v20.10.15a1` (note: postfix `a1`);
    - wait for green build in [Actions](https://github.com/neuro-inc/neuro-extras/actions);
    - verify that docker image was updated on [DockerHub](https://hub.docker.com/r/neuromation/neuro-extras/tags);
    - Note: `pip install -U neuro-extras` won't upgrade as it's an alpha release;

## Part 2: full-release

3. Use the newly updated docker image inside the code.
    - `git checkout master`;
    - set the new alpha version `v20.10.15a1` to variable `NEURO_EXTRAS_IMAGE_TAG` in `neuro_extras/main.py`;
    - `git commit -m "Use image version to v20.10.15a1"` and `push` directly to `origin/master`;

4. Bump non-alpha version in the code.
    - `git checkout master`;
    - update `neuro_extras/__init__.py`: `__version__ = "v20.10.15"` (note: no postfixes);
    - run `make changelog-draft` and verify changelog looks valid;
    - run `make changelog` - this will delete changelog items from `CHANGELOG.d`;
    - `git add . && git commit -m "Release version v20.10.15"` and `push` directly to `origin/master`;
    - wait for green build in [Actions](https://github.com/neuro-inc/neuro-extras/actions);

5. Submit a release on GitHub.
    - go to [Releases](https://github.com/neuro-inc/neuro-extras/releases/), `Draft a new release`, tag: `v20.10.15` (note: no postfixes);
    - wait for green build in [Actions](https://github.com/neuro-inc/neuro-extras/actions);
    - verify that `pip install -U neuro-extras` does install `neuro-extras==20.10.15`