from neuromation.api import PluginManager

from .main import main  # noqa


__version__ = "20.9.25"


def setup_plugin(manager: PluginManager) -> None:
    manager.config.define_str("extra", "remote-project-dir")
