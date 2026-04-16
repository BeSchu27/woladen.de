from .config import AppConfig
from .service import IngestionService

__all__ = ["AppConfig", "IngestionService", "create_app"]


def create_app(*args, **kwargs):
    from .api import create_app as _create_app

    return _create_app(*args, **kwargs)
