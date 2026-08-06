"""Managed adapter distribution boundaries."""

from .manifest import KitComponent, KitManifest, load_kit_manifest
from .setup import SetupPlan, apply_setup, build_setup_plan

__all__ = [
    "KitComponent",
    "KitManifest",
    "SetupPlan",
    "apply_setup",
    "build_setup_plan",
    "load_kit_manifest",
]
