# Versioning

The project uses release versioning a format YY.MM.DD. For instance, when releasing a version on Sep 15, 2021, it would be versionsed as 21.9.15. Note there's no leading zeroes in day and month number.

# Release process
The release process is logically split onto two steps: firstly we release docker image with an updated code,
  afterwards we release an updated `neuro-extras` Python package to PyPi.

##### Note: Start with step 1 if the docker image should be updated. Otherwise - start with step 5.

1. Make sure all tests are green in master branch.
2. Bump version in `neuro_extras/__init__.py` to an `alpha version` (postfixed with aN where N is a number), commit & push.
3. Submit pre-release on GitHub with a corresponding alpha. 
It will trigger CI/CD, test and publish a new docker image and alpha release in PyPi (docker's `neuromation/neuro-extras:latest` won't be an alpha image and `pip install -U neuro-extras` won't upgrade on an alpha version).
If all is OK, you could move to the next step, where you publish `neuro-extras` Python package to PyPi, which will use the previously published docker image. 
4. Bump version of docker image in `neuro_extras/main.py` to the previously published `alpha version`.
5. Bump version in `neuro_extras/__init__.py` to the `normal version`.
6. Run `make changelog-draft` and verify changelog looks valid
7. Run `make changelog` - this will delete changelog items from `CHANGELOG.d` and create a commit with updated CHANGELOG.md file
8. Commit proposed changelog changes & push to GitHub.
9. Create a release on GitHub. Make sure to prefix your version number with `v`, i.e. for version 21.9.15 you would name your release `v21.9.15`
10. Wait for corresponding GitHub Action to complete.
