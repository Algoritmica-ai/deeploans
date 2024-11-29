from fastapi import APIRouter

from app.config import API_VERSION as version
from app.dev.dev_tools import router as dev_router

router = APIRouter()

router.include_router(dev_router, prefix=f"/api/{version}/dev")
