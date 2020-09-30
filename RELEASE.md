# Versioning

The project uses release versioning a format YY.MM.DD. For instance, when releasing a version on Sep 15, 2021, it would be versionsed as 21.9.15. Note there's no leading zeroes in day and month number.

# Release process

1. Make sure all tests are green
2. Set correct version in `neuro_extras/__init__.py`
3. Run `make changelog-draft` and verify changelog looks valid
4. Run `make changelog` - this will delete changelog items from `CHANGELOG.d` and create a commit with updated CHANGELOG.md file
5. Commit proposed changelog changes.
5. Push to GitHub
6. Create a release on GitHub. Make sure to prefix your version number with `v`, i.e. for version 21.9.15 you would name your release `v21.9.15`
7. Wait for corresponding GitHub Action to complete.