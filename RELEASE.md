# Versioning

The project uses release versioning a format `YY.MM.DD` (both for pip packages and image tags). For instance, when releasing a version on Sep 15, 2021, it would be versionsed as `21.9.15` (no zero before `9`).


# Release process

Suppose, today is October 15, 2020, and we want to update both neuro-extras pip package and docker image.

0. Make sure all tests are green in `master` branch.

1. Bump code version directly to `master`.
    - `git checkout master`;
    - update `neuro_extras/version.py`: set `__version__ = "20.10.15"`;
    - run `make changelog-draft` and verify changelog looks valid;
    - run `make changelog` - this will delete changelog items from `CHANGELOG.d`;
    - `git add CHANGELOG* neuro_extras/version.py && git commit -m "Release version 20.10.15"` and `push` directly to `origin/master`;
    - wait for green build in [Actions](https://github.com/neuro-inc/neuro-extras/actions);

2. Submit a release on GitHub.
    - go to [Releases](https://github.com/neuro-inc/neuro-extras/releases/), `Draft a new release`, tag: `20.10.15` (note: no postfixes);
    - wait for green build in [Actions](https://github.com/neuro-inc/neuro-extras/actions);
    - if the build failed, fix the errors and repeat from step 1. with version postfix: `20.10.15-1`.
    - verify that `pip install -U neuro-extras` does install `neuro-extras==20.10.15`


# Alpha release

To debug a release process without changing latest versions of package and image, you can issue an alpha release just postfixing the version with `a`:
- `20.10.15a1`
- `20.10.15a2`
