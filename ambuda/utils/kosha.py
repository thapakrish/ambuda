import functools
from pathlib import Path

from flask import current_app
from vidyut.kosha import Kosha


def get_kosha():
    """Load a kosha (no singleton, for throwaway instances in celery)."""
    return Kosha(Path(current_app.config["VIDYUT_DATA_DIR"]) / "kosha")
