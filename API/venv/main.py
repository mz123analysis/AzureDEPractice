from fastapi import FastAPI, Depends, Query, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.hash import bcrypt
from jose import JWTError, jwt
from tortoise.exceptions import DoesNotExist
from tortoise import fields
from tortoise.models import Model
from tortoise.contrib.fastapi import register_tortoise
from tortoise.contrib.pydantic import pydantic_model_creator # Helps us validiates inputs vs database 
from db import get_connection
from datetime import date
import datetime

app = FastAPI()

JWT_SECRET = 'mysecret' #Example of JWT Secret, but should be an env variable

# Building a Class to Create a User tortoise model
class Users(Model):
    id = fields.IntField(pk=True)  
    username = fields.CharField(50, unique = True)
    password_hash = fields.CharField(128)
    access = fields.CharField(50)

    def verify_password(self,password):
        return bcrypt.verify(password, self.password_hash)

User_Pydantic = pydantic_model_creator(Users, name = 'Users')
UserIn_Pydantic = pydantic_model_creator(Users, name = 'UsersIn', exclude_readonly=True)

@app.post('/users', response_model=User_Pydantic)
async def create_user(user: UserIn_Pydantic):
    user_obj = Users(username = user.username, password_hash=bcrypt.hash(user.password_hash), access = user.access)
    await user_obj.save()
    return await User_Pydantic.from_tortoise_orm(user_obj)

register_tortoise(
    app,
    db_url='sqlite://fakedb.sqlite3',
    modules={'model':['main']},
    generate_schemas=True,
    add_exception_handlers=True
)

oauth2_scheme =  OAuth2PasswordBearer(tokenUrl='token')

@app.post('/token')
async def generate_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = await authenticate_user(form_data.username, form_data.password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user_dict = {"id": user.id, "username": user.username, "access":user.access}

    expiration_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=15) # Token expires in 15 minutes

    token = jwt.encode({"id":user_dict["id"], "username":user_dict["username"], "access" : user_dict['access'], "exp": expiration_time}, JWT_SECRET)
    return {'access_token': token, 'token_type': 'bearer'}


async def authenticate_user(username: str, password: str):
    user = await Users.get(username=username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.verify_password(password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
        user_id = payload.get("id")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = await Users.get_or_none(id=user_id)
    if user is None:
        raise credentials_exception

    return user 

@app.get("/data")
async def read_ProdTable(
    start_date: date = Query(..., description="Start date in YYYY-MM-DD"),
    end_date: date = Query(..., description="End date in YYYY-MM-DD"),
    current_user: Users = Depends(get_current_user)
):
    if current_user.access != "read":
        raise HTTPException(status_code=403, detail="Not authorized")
    
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT * FROM dbo.ProdTable
            WHERE DATE BETWEEN ? AND ?
        """
        cursor.execute(query, start_date, end_date)
    
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        return results
    finally:
        cursor.close()
        conn.close()