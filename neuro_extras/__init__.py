from neuro_sdk import PluginManager

from .main import main  # noqa
from .version import __version__  # noqa


NEURO_EXTRAS_UPGRADE = """\
You are using Neuro Extras tool {old_ver}, however {new_ver} is available.
You should consider upgrading via the following command:
    python -m pip install --upgrade neuro-extras
"""


def get_neuro_extras_txt(old: str, new: str) -> str:
    return NEURO_EXTRAS_UPGRADE.format(old_ver=old, new_ver=new)


def setup_plugin(manager: PluginManager) -> None:
    manager.config.define_str("extra", "remote-project-dir")

    manager.version_checker.register("neuro-extras", get_neuro_extras_txt)
