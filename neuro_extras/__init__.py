from neuromation.api import PluginManager

from .main import main  # noqa


__version__ = "20.9.22a2"


def setup_plugin(manager: PluginManager) -> None:
    manager.config.define_str("extra", "remote-project-dir")
