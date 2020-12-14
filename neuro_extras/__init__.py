from neuro_sdk import PluginManager

from .main import main  # noqa
from .version import __version__  # noqa


def setup_plugin(manager: PluginManager) -> None:
    manager.config.define_str("extra", "remote-project-dir")
