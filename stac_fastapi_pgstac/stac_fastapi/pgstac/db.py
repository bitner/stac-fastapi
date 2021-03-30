import logging
import asyncpg

from stac_fastapi.pgstac.config import Settings

settings=Settings()

from fastapi import FastAPI

logger = logging.getLogger(__name__)

async def connect_to_db(app: FastAPI) -> None:
    """Connect."""
    logger.info(f"Connecting  read pool to {settings.reader_connection_string}")
    app.state.readpool = await asyncpg.create_pool(
        settings.reader_connection_string,
        min_size=settings.db_min_conn_size,
        max_size=settings.db_max_conn_size,
        max_queries=settings.db_max_queries,
        max_inactive_connection_lifetime=settings.db_max_inactive_conn_lifetime,
    )
    logger.info("Connection to read pool established")
    logger.info(f"Connecting write pool to {settings.writer_connection_string}")

    app.state.writepool = await asyncpg.create_pool(
        settings.writer_connection_string,
        min_size=settings.db_min_conn_size,
        max_size=settings.db_max_conn_size,
        max_queries=settings.db_max_queries,
        max_inactive_connection_lifetime=settings.db_max_inactive_conn_lifetime,
    )
    logger.info("Connection to write pool established")


async def close_db_connection(app: FastAPI) -> None:
    """Close connection."""
    logger.info("Closing connections to database")
    await app.state.readpool.close()
    await app.state.writepool.close()
    logger.info("Connections closed")