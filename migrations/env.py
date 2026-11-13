import asyncio

from alembic import context

from app.db import engine
from app.models import Base


def run_sync(connection):
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run():
    async with engine.connect() as connection:
        await connection.run_sync(run_sync)
    await engine.dispose()


asyncio.run(run())
