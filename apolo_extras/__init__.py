from apolo_sdk import PluginManager

from .main import main  # noqa
from .version import __version__  # noqa


APOLO_EXTRAS_UPGRADE = """\
You are using apolo-extras tool {old_ver}, however {new_ver} is available.
You should consider upgrading via the following command:
    python -m pip install --upgrade apolo-extras
"""


def get_apolo_extras_txt(old: str, new: str) -> str:
    return APOLO_EXTRAS_UPGRADE.format(old_ver=old, new_ver=new)


def setup_plugin(manager: PluginManager) -> None:
    manager.version_checker.register("apolo-extras", get_apolo_extras_txt)
