# Initial setup

To launch the app (even locally) you need bigquery credentials in `bigquery.json` file in `backend/` directory

Setup is pretty easy - for local development just create `.env` file with fields from `.env.template` in the same directory and use docker-compose. 

For prod deployment - you can recreate docker-compose setup on the server (of course scaled to fit the needs), but without mongodb-express (it`s only for development).

Additional configurations can be found in `backend/app/config.py`

# Endpoints documentation

You can access the Swagger docs of all non-admin endpoints at `/docs` endpoint. 

To use admin endpoints you have to pass into `x-algoritmica-api-key` the `DEV_API_KEY` key, not user one. Below is the short documentation of those endpoints with examples of responses. 

#### GET /api/v1/dev/users
List the data of all users.

**Response example:**
```json    
[
    {
        "id": "64d628aadbb85ed6007c1e61",
        "email": "test@email.com",
        "api_key": "hZ_vOEu0JeN8dsvQ",
        "quota": 1000,
        "used_quota": 20
    },
    {
        "id": "64d628aadbb85ed6007c1e45",
        "email": "test2@email.com",
        "api_key": "hZ_vOEu0JsdfRXvQ",
        "quota": 100,
        "used_quota": 0
    }
]
```
&nbsp;
#### GET /api/v1/dev/user/<user_id>
List the data of a user with given id.

**Response example:**
```json
{
    "id": "64d628aadbb85ed6007c1e61",
    "email": "test@email.com",
    "api_key": "hZ_vOEu0JeN8dsvQ",
    "quota": 1000,
    "used_quota": 20
}
```
&nbsp;
#### POST /api/v1/dev/users
Create a new user with optional configs (other that defaults from `.env` file).

**Body example:**
```sh
{
    "email": "tesssdsassss@email.com", 
    "quota": 220, # OPTIONAL
    "used_quota": 10, # OPTIONAL
    "api_key": "API_KEY", # OPTIONAL
}
```

**Response example:**
```json
{
    "id": "64d628aadbb85ed6007c1e61",
    "email": "test@email.com",
    "api_key": "hZ_vOEu0JeN8dsvQ",
    "quota": 1000,
    "used_quota": 20
}
```
&nbsp;
#### PUT /api/v1/dev/user/<user_id>
Update the user - you cannot change user`s email and id!

**Body example:**
```sh
{
    "email": "tesssdsassss@email.com",
    "api_key": "NEW_KEY",
    "quota": 400,
    "used_quota": 200
}
```

**Response example:**
```json
{
    "id": "64f748e3cbbf80551674c9ef",
    "email": "tesssdsassss@email.com",
    "api_key": "NEW_KEY",
    "quota": 400,
    "used_quota": 200
}
```
&nbsp;
#### DELETE /api/v1/dev/user/<user_id>
Delete a user with given id - this operation is irreversible!

**Response example:**
```json
{
    "Ok": "Deleted user with id = 64f748e3cbbf80551674c9ef"
}
```
&nbsp;