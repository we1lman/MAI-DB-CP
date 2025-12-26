from __future__ import annotations

from fastapi import FastAPI
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.api.router import router as api_router
from app.errors import translate_db_error

app = FastAPI(
    title="Metrology DB-first API",
    version="0.1.0",
)

app.include_router(api_router)


@app.exception_handler(IntegrityError)
async def handle_integrity_error(_: Request, exc: IntegrityError) -> JSONResponse:
    status, payload = translate_db_error(exc)
    return JSONResponse(status_code=status, content=payload)


@app.exception_handler(SQLAlchemyError)
async def handle_sqlalchemy_error(_: Request, exc: SQLAlchemyError) -> JSONResponse:
    status, payload = translate_db_error(exc)
    return JSONResponse(status_code=status, content=payload)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


