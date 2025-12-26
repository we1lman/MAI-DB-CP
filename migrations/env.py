from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Alembic Config object (provides access to .ini file values)
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# DB-first: metadata is intentionally empty (DDL via raw SQL in migrations)
target_metadata = None


def get_url() -> str:
    url = os.getenv("DATABASE_URL_SYNC")
    if not url:
        raise RuntimeError("DATABASE_URL_SYNC is not set (see env.template)")
    return url


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=False,
        compare_server_default=False,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=False,
            compare_server_default=False,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()


