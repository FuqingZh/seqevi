"""Content-addressed protein sequence annotation evidence."""

from importlib.metadata import version


__version__ = version("seqevi")

from .api import annotate, scan_annotations

__all__ = ["__version__", "annotate", "scan_annotations"]
