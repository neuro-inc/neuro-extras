[comment]: # (Please do not modify this file)
[comment]: # (Put your comments to changelog.d and it will be moved to changelog in next release)
[comment]: # (Clear the text on make release for canceling the release)

[comment]: # (towncrier release notes start)

neuro-extras v24.5.1 (2024-05-28)
=================================



Bugfixes
--------

- Fix image build in organization when user has no direct access to cluster ([#603](https://github.com/neuro-inc/neuro-extras/pull/603))



neuro-extras v24.5.0 (2024-05-22)
=================================


No significant changes.


neuro-extras v24.2.0 (2024-02-13)
=================================


Features
--------


- Updated Kaniko to 1.20.0, allowed to provide extra arguments for Kaniko executor. ([#601](https://github.com/neuromation/neuro-extras/issues/601))


neuro-extras v23.11.0 (2023-11-01)
==================================


Features
--------


- Use `-p/--project` while building image to set the project for entire build process. ([#600](https://github.com/neuromation/neuro-extras/issues/600))


neuro-extras v23.7.1 (2023-07-13)
=================================


Bugfixes
--------


- Fix runnning Kaniko on some envs ([#599](https://github.com/neuro-inc/neuro-extras/pull/599))


neuro-extras v23.7.0 (2023-07-06)
=================================

Features
--------

- Added project support. ([#595])(https://github.com/neuro-inc/neuro-extras/issues/595))


Bugfixes
--------

- Fixed data cp to s3 destination not working. ([#301](https://github.com/neuromation/neuro-extras/issues/301))

- Fixed a bug with data cp to s3 with compression causing `$HOME/` directory wipe. ([#500](https://github.com/neuromation/neuro-extras/issues/500))


Misc
----

- [#506](https://github.com/neuromation/neuro-extras/issues/506)


neuro-extras v22.2.2 (2022-02-17)
=================================


Bugfixes
--------


- Fix relative dockerfile path for Kaniko on Windows ([#463](https://github.com/neuromation/neuro-extras/issues/463))


neuro-extras v22.2.1 (2022-02-10)
=================================


Features
--------


- Added Python 3.10 support. ([#417](https://github.com/neuromation/neuro-extras/issues/417))

- Add `local-build` subcommand to `neuro-extras image` that allows building images via local Docker daemon. ([#448](https://github.com/neuromation/neuro-extras/issues/448))

- Help user to select the resource preset for image build ([#450](https://github.com/neuromation/neuro-extras/issues/450))


Deprecations and Removals
-------------------------


- Removed Python 3.6 support. ([#417](https://github.com/neuromation/neuro-extras/issues/417))


Neuro_Extras 21.11.0 (2021-11-23)
=================================


Features
--------


- Allow to build and push docker images to the remote image registries.

  The requirement for allowing Kaniko (the underlying tool) to push such images - it should be authenticated.

  The registry authention data structure should be in the following form:
  `{"auths": {"<registry URI>": {"auth": "<base64 encoded '<username>:<password>' string"}}}`
  (tested with the `https://index.docker.io/v1/` - public DockerHub registry)

  `neuro-extras config build-registy-auth` command might become handy in this case.

  To attach the target registy AUTH data into the builder job, one might save it as the platform secret and mount into the builder job.
  Mounting of the secret could be done either as the ENV variable with the name preffixed by `NE_REGISTRY_AUTH`, or as the secret file.
  In the later case, the ENV variable should also be added with the mentioned above preffix and pointing to the corresponding file. ([#328](https://github.com/neuromation/neuro-extras/issues/328))


neuro-extras v21.9.3 (2021-09-03)
=================================


Features
--------


- Support Python 3.8 and 3.9 ([#249](https://github.com/neuromation/neuro-extras/issues/249))


neuro-extras v21.7.23 (2021-07-23)
=================================


Features
--------


- Reduce neuro-extras image size, add neuro-flow into the image and include GitHub ssh key to known hosts. ([#290](https://github.com/neuromation/neuro-extras/issues/290))


Bugfixes
--------


- Image transfer now works even if it was initiated not from SRC cluster. ([#295](https://github.com/neuromation/neuro-extras/issues/295))


neuro-extras v21.7.2 (2021-07-02)
=================================


Features
--------


- Make `neuro-extras data cp` accept preset name and migrate code to use `neuro.jobs.start` method in the SDK ([#218](https://github.com/neuromation/neuro-extras/issues/218))


Bugfixes
--------


- Downgrade Kaniko to v1.3.0 due to OOMKills ([#287](https://github.com/neuromation/neuro-extras/issues/287))


Neuro_Extras 21.3.19 (2021-03-19)
=================================

Bugfixes
--------


- Fix `neuro-extras data transfer` job lifespan (set to 10 days), add usage hints, fix seldon init container user ID ([#187](https://github.com/neuromation/neuro-extras/issues/187))

- Fix connection issues in image build jobs by relying on neuro-cli instead of neuro-sdk, add support of job tags for image build jobs. ([#205](https://github.com/neuromation/neuro-extras/issues/205))


Misc
----

- [#211](https://github.com/neuromation/neuro-extras/issues/211)


Neuro_Extras 20.12.16 (2020-12-16)
==================================

No significant changes.


Neuro_Extras 20.12.15 (2020-12-15)
==================================

No significant changes.


Neuro_Extras 20.11.20 (2020-11-20)
==================================

No significant changes.


Neuro_Extras 20.11.13 (2020-11-13)
=================================

Features
--------


- Reduce verbosity for image building (to turn it back on: `neuro-extras image build --verbose ...`). ([#84](https://github.com/neuromation/neuro-extras/issues/84))

- Add `--preset` parameter to `neuro-extras image build` subcommand in order to specify preset ([#136](https://github.com/neuromation/neuro-extras/issues/136))

- Add support for HTTP(S) data ingestion ([#150](https://github.com/neuromation/neuro-extras/issues/150))


Bugfixes
--------


- Speedup image build by Kaniko downgrade to v1.1.0 (COPY layers caching), replace /workspace context dir to /kaniko_context. ([#157](https://github.com/neuromation/neuro-extras/issues/157))


Neuro_Extras 20.11.1 (2020-11-01)
=================================

Features
--------


- Bump neuromation to >=20.10.30 ([#141](https://github.com/neuromation/neuro-extras/issues/141))


Neuro_Extras 20.10.28 (2020-10-28)
==================================

Bugfixes
--------


- Fix exit-code for image builder job ([#82](https://github.com/neuromation/neuro-extras/issues/82))

- Support full storage URI for both SRC and DST in `neuro-extras data transfer` ([#112](https://github.com/neuromation/neuro-extras/issues/112))

- Fix data ingestion to/from GCP (move GCS auth to entrypoint) & add hints to CLI commands. ([#115](https://github.com/neuromation/neuro-extras/issues/115))

- data cp: fix source archive removal if SRC is a local file; add flag for optional TMP folder usage ([#118](https://github.com/neuromation/neuro-extras/issues/118))

- Fix image transfer: switch cluster properly ([#130](https://github.com/neuromation/neuro-extras/issues/130))



Neuro_Extras v20.10.16 (2020-10-16)
===================================

Features
--------


- Add `-c` / `--compress` option to `data cp` to support data packing before sending to destination ([#38](https://github.com/neuromation/neuro-extras/issues/38))


Neuro_Extras v20.9.30.2 (2020-09-30)
====================================

No significant changes.


Neuro_Extras v20.9.30 (2020-09-30)
==================================

Features
--------


- Change image's entrypoint from `neuro` to `bash`. ([#81](https://github.com/neuromation/neuro-extras/issues/81))



Neuro_Extras v20.9.29a2 (2020-09-29)
====================================

Features
--------


- Add `towncrier` for release notes management ([#78](https://github.com/neuromation/neuro-extras/issues/78))
