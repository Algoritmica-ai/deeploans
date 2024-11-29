from fastapi import APIRouter
from ..model import user as user_model

router = APIRouter()
tag = 'dev'

"""
USERS
"""


@router.get("/users", tags=[tag], include_in_schema=False)
async def get_users():
    return user_model.get_users()


@router.get("/user/{user_id}", tags=[tag], include_in_schema=False)
async def get_user(user_id: str):
    result = user_model.get_user_by_id(user_id)
    return result.model_dump()


@router.post("/users", tags=[tag], include_in_schema=False)
async def add_new_user(user: user_model.User):
    result = user_model.add_new_user(user)
    return result.model_dump()


@router.put("/user/{user_id}", tags=[tag], include_in_schema=False)
async def edit_user(user_id: str, user: user_model.User):
    result = user_model.update_user(user_id, user)
    return result.model_dump()


@router.delete("/user/{user_id}", tags=[tag], include_in_schema=False)
async def delete_user(user_id: str):
    user_model.delete_user(user_id)
    return {'Ok': f'Deleted user with id = {user_id}'}
