Neuro_Extras 20.11.20 (2020-11-20)
==================================

No significant changes.


Neuro_Extras 20.11.1 (2020-11-13)
=================================

Features
--------


- Reduce verbosity for image building (to turn it back on: `neuro-extras image build --verbose ...`). ([#84](https://github.com/neuromation/neuro-extras/issues/84))

- Add `--preset` parameter to `neuro-extras image build` subcommand in order to specify preset ([#136](https://github.com/neuromation/neuro-extras/issues/136))

- Add support for HTTP(S) data ingestion ([#150](https://github.com/neuromation/neuro-extras/issues/150))


Bugfixes
--------


- Speedup image build by Kaniko downgrade to v1.1.0 (COPY layers caching), replace /workspace context dir to /kaniko_context. ([#157](https://github.com/neuromation/neuro-extras/issues/157))


Neuro_Extras 20.10.28 (2020-10-28)
==================================

Bugfixes
--------


- Fix exit-code for image builder job ([#82](https://github.com/neuromation/neuro-extras/issues/82))

- Support full storage URI for both SRC and DST in `neuro-extras data transfer` ([#112](https://github.com/neuromation/neuro-extras/issues/112))

- Fix data ingestion to/from GCP (move GCS auth to entrypoint) & add hints to CLI commands. ([#115](https://github.com/neuromation/neuro-extras/issues/115))

- data cp: fix source archive removal if SRC is a local file; add flag for optional TMP folder usage ([#118](https://github.com/neuromation/neuro-extras/issues/118))

- Fix image transfer: switch cluster properly ([#130](https://github.com/neuromation/neuro-extras/issues/130))


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


[comment]: # (Please do not modify this file)
[comment]: # (Put you comments to changelog.d and it will be moved to changelog in next release)

[comment]: # (Clear the text on make release for canceling the release)

[comment]: # (towncrier release notes start)

Neuro_Extras 20.12.16 (2020-12-16)
==================================

No significant changes.


Neuro_Extras 20.12.15 (2020-12-15)
==================================

No significant changes.


Neuro_Extras 20.11.1 (2020-11-01)
=================================

Features
--------


- Bump neuromation to >=20.10.30 ([#141](https://github.com/neuromation/neuro-extras/issues/141))


Neuro_Extras v20.10.16 (2020-10-16)
===================================

Features
--------


- Add `-c` / `--compress` option to `data cp` to support data packing before sending to destination ([#38](https://github.com/neuromation/neuro-extras/issues/38))


Neuro_Extras v20.9.30.2 (2020-09-30)
====================================

No significant changes.
