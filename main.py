"""Compatibility entrypoint for the internal read-only API.

The FastAPI implementation lives in ``data_platform.api.server``. This wrapper
keeps the existing ``uvicorn main:app`` command stable for collaborators.
"""

from data_platform.api.server import app

__all__ = ["app"]
