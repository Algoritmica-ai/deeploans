import secrets
from bson import ObjectId
from app.memory import memory
from app.config import DEFAULT_CALLS_QUOTA, INVALID_API_KEY_MESSAGE

from fastapi import HTTPException
from pydantic import BaseModel, Field


class User(BaseModel):
    id: str | None = None
    email: str = Field(...)
    api_key: str | None = None
    quota: int = DEFAULT_CALLS_QUOTA
    used_quota: int = 0

    @staticmethod
    def generate_api_key() -> str:
        # Randomly generate email and API key using secrets
        api_key = secrets.token_urlsafe(12)
        return api_key


"""
CREATE
"""


def add_new_user(user: User) -> User:
    query = {"email": user.email}
    if memory.mongo_users.count_documents(query, limit=1):  # Email already in db
        raise HTTPException(status_code=409, detail=f"Email {user.email} already in database.")

    api_key = User.generate_api_key()
    user.api_key = api_key
    
    inserted_id = memory.mongo_users.insert_one(user.model_dump()).inserted_id
    user.id = str(inserted_id)
    return user


"""
READ
"""


def get_users() -> list:
    users = []
    results = list(memory.mongo_users.find())
    for r in results:
        r["id"] = str(r["_id"])
        users.append(User(**r).model_dump())
    return users


def get_user_by_id(user_id: str) -> User:
    query = {"_id": ObjectId(user_id)}
    if memory.mongo_users.count_documents(query, limit=1):
        r = memory.mongo_users.find_one(query)
        r["id"] = str(r["_id"])
        return User(**r)
    else:
        raise HTTPException(status_code=404, detail=f"Id {user_id} not found")


"""
UPDATE
"""


def update_user(user_id: str,
                user: User) -> User:
    user_id = ObjectId(user_id)
    query = {"_id": user_id}

    if memory.mongo_users.count_documents(query, limit=1):
        r = memory.mongo_users.find_one(query)
        old_user = User(**r)
        if old_user.email != user.email:
            raise HTTPException(status_code=400, detail=f"Cannot change user email")

        memory.mongo_users.update_one({"_id": user_id}, {"$set": user.model_dump()})
        user.id = str(user_id)
        return user

    else:
        raise HTTPException(status_code=404, detail=f"Id {str(user_id)} not found")


def update_used_quota(api_key: str,
                      increment_by: int = 1) -> str | None:
    # Find user with this key
    query = {"api_key": api_key}

    if memory.mongo_users.count_documents(query, limit=1):
        r = memory.mongo_users.find_one(query)
        user = User(**r)

        # Check if user has enough remaining credits
        if user.used_quota + increment_by > user.quota:
            return "Not enough credits"

        user.used_quota = user.used_quota + increment_by
        memory.mongo_users.update_one(query, {"$set": user.model_dump()})

    else:
        return INVALID_API_KEY_MESSAGE

    return None


"""
DELETE
"""


def delete_user(user_id: str):
    query = {"_id": ObjectId(user_id)}
    memory.mongo_users.delete_one(query)
