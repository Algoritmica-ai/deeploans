from ..routers import HandleTrailingSlashRouter

from app.config import API_VERSION as version
# from app.routes.aut import router as aut_router
# from app.routes.cmr import router as cmr_router
# from app.routes.cre import router as cre_router
# from app.routes.les import router as les_router
# from app.routes.rmb import router as rmb_router
from app.routes.sme import router as sme_router

router = HandleTrailingSlashRouter()

# router.include_router(aut_router, prefix=f"/api/{version}/aut")
# router.include_router(cmr_router, prefix=f"/api/{version}/cmr")
# router.include_router(cre_router, prefix=f"/api/{version}/cre")
# router.include_router(les_router, prefix=f"/api/{version}/les")
# router.include_router(rmb_router, prefix=f"/api/{version}/rmb")
router.include_router(sme_router, prefix=f"/api/{version}/sme")
