import logging
import sys

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from app import config, server_startup_tasks
from app.middleware import AuthenticationMiddleware
from app.routes import router as api_router
from app.dev import router as dev_router

"""
LOGGER
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

"""
FastAPI App
"""
tags_metadata = [
    dict(name='sme',
         description="SME credit type"),
    dict(name='dev',
         description='Dev tools - do not share')
]

def customOpenAPI():
    openapiSchema = get_openapi(
        title = app.title,
        openapi_version = "3.0.0",
        version = app.version,
        summary = app.summary,
        description = app.description,
        routes = app.routes
    )

    app.openapi_schema = openapiSchema
    return app.openapi_schema


app = FastAPI(title=config.PROJECT_NAME, version=config.VERSION, openapi_tags=tags_metadata)
app.include_router(api_router)
app.include_router(dev_router)
app.add_middleware(AuthenticationMiddleware)
app.add_event_handler("startup", server_startup_tasks.create_start_app_handler())
app.add_event_handler("shutdown", server_startup_tasks.create_exit_app_handler())
app.openapi = customOpenAPI


@app.get("/", include_in_schema=False)
async def root():
    return {'Hello': 'World'}
