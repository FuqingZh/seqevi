"""Content-addressed protein sequence annotation evidence."""

__version__ = "0.3.3"

from .api import annotate, scan_annotations

__all__ = ["__version__", "annotate", "scan_annotations"]
