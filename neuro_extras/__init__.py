from neuromation.api import PluginManager

from .main import main  # noqa


# NOTE: When updating the version, don't forget to update main.NEURO_EXTRAS_IMAGE_TAG
__version__ = "v20.10.14a1"


def setup_plugin(manager: PluginManager) -> None:
    manager.config.define_str("extra", "remote-project-dir")
