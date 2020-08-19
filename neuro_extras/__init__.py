from neuromation.api import PluginManager

from .main import main  # noqa


__version__ = "20.8.19"


def setup_plugin(manager: PluginManager) -> None:
    manager.config.define_str("extra", "remote-project-dir")
