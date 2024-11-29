from starlette.middleware.base import BaseHTTPMiddleware, Request
from starlette.responses import Response
from starlette.status import HTTP_403_FORBIDDEN

from app.config import API_KEY_HEADER, DEV_API_KEY, INVALID_API_KEY_MESSAGE
from .model.user import update_used_quota


class AuthenticationMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)

    async def set_body(self, request: Request):
        receive_ = await request._receive()

        async def receive():
            return receive_

        request._receive = receive

    async def dispatch(self, request, call_next):
        await self.set_body(request)
        # body = await request.body()

        # Passthrough for swagger
        if request.url.path.split('/')[1] in ['docs', 'openapi.json']:
            return await call_next(request)

        # Get API key from header
        api_key_from_request = request.headers.get(API_KEY_HEADER, None)
        if not api_key_from_request:
            return Response(status_code=HTTP_403_FORBIDDEN, content=INVALID_API_KEY_MESSAGE)

        # Make sure regular users don't access DEV tools
        if '/dev/' in request.url.path and api_key_from_request != DEV_API_KEY:
            return Response(status_code=HTTP_403_FORBIDDEN, content=INVALID_API_KEY_MESSAGE)

        # Passthrough dev api requests
        if api_key_from_request == DEV_API_KEY:
            return await call_next(request)

        # Regular user
        res = update_used_quota(api_key=api_key_from_request)

        if res:  # Error message
            return Response(status_code=HTTP_403_FORBIDDEN, content=res)

        return await call_next(request)
