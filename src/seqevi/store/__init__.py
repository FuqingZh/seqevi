"""Evidence Store implementations and public local Store contract."""

from .local import LocalStore, resolve_store_path

__all__ = ["LocalStore", "resolve_store_path"]
