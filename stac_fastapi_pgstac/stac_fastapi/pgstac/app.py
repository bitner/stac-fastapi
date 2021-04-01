"""FastAPI application using PGStac."""
from stac_fastapi.api.app import StacApi
from stac_fastapi.api.routes import create_async_endpoint
from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.db import connect_to_db, close_db_connection
from stac_fastapi.pgstac.core import CoreCrudClient

settings = Settings()

api = StacApi(
    settings=settings,
    extensions=[
    ],
    client=CoreCrudClient(),
    endpoint_factory=create_async_endpoint,
)
app = api.app

@app.on_event("startup")
async def startup_event():
    """ Connect to database on startup """
    await connect_to_db(app)

@app.on_event("shutdown")
async def shutdown_event():
    await close_db_connection(app)