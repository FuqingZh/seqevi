"""Content-addressed protein sequence annotation evidence."""

__version__ = "0.3.2"

from .api import annotate, scan_annotations

__all__ = ["__version__", "annotate", "scan_annotations"]
