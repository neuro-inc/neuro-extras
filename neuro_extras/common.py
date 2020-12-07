import os

from .version import __version__


NEURO_EXTRAS_IMAGE = os.environ.get(
    "NEURO_EXTRAS_IMAGE", f"neuromation/neuro-extras:{__version__}"
)
