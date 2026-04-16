from __future__ import annotations

import importlib
import sys


def test_backend_package_defers_api_import() -> None:
    sys.modules.pop("backend", None)
    sys.modules.pop("backend.api", None)

    backend = importlib.import_module("backend")

    assert "backend.api" not in sys.modules
    assert callable(backend.create_app)
