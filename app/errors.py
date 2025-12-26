from __future__ import annotations

from typing import Any

from sqlalchemy.exc import DBAPIError, IntegrityError


def translate_db_error(exc: Exception) -> tuple[int, dict[str, Any]]:
    """
    Convert DB/SQLAlchemy exceptions into safe HTTP payloads.

    Returns: (status_code, json_payload)
    """
    # Default: hide internals
    payload: dict[str, Any] = {"error": "db_error"}

    if isinstance(exc, IntegrityError):
        orig = getattr(exc, "orig", None)
        constraint = getattr(orig, "constraint_name", None) if orig is not None else None

        # asyncpg exposes typed exceptions; map them when available
        try:
            from asyncpg.exceptions import (  # type: ignore
                CheckViolationError,
                ForeignKeyViolationError,
                NotNullViolationError,
                UniqueViolationError,
            )
        except Exception:  # pragma: no cover
            UniqueViolationError = ForeignKeyViolationError = CheckViolationError = NotNullViolationError = ()  # type: ignore

        if orig is not None and isinstance(orig, UniqueViolationError):
            return 409, {"error": "unique_violation", "constraint": constraint}
        if orig is not None and isinstance(orig, ForeignKeyViolationError):
            return 409, {"error": "foreign_key_violation", "constraint": constraint}
        if orig is not None and isinstance(orig, CheckViolationError):
            return 400, {"error": "check_violation", "constraint": constraint}
        if orig is not None and isinstance(orig, NotNullViolationError):
            return 400, {"error": "not_null_violation", "constraint": constraint}

        # Fallback for other integrity issues
        return 400, {"error": "integrity_error", "constraint": constraint}

    if isinstance(exc, DBAPIError):
        return 500, payload

    return 500, payload


