from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from app.settings import settings

engine = create_async_engine(
    settings.database_url_async,
    pool_pre_ping=True,
)


async def get_conn() -> AsyncIterator[AsyncConnection]:
    async with engine.connect() as conn:
        yield conn


