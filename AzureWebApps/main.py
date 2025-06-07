from fastapi import FastAPI, Depends, Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from db import get_connection
from security import verify_api_key
from datetime import date


app = FastAPI()
## Use Case for Data Engineer
## Utilize OAuth2 and JWT to authenticate and authorize what type of data clients can see 
## OAuth2 would allow me to compare input to hashed data from database
## JWT would allow clients to get the payload (user,id,access level, etc.)
## As owner of API, I will create the username and password and send it to the client 
## From there, clients can authenticate and see end points they are only authorized to see
## ** Must build end points with different access levels in mind **
"""
Class User(Model)

oauth2_scheme =  OAuth2PasswordBearer(tokenUrl='token')
@app.post('/token')
async def token(form_data: OAuth2PasswordRequestForm = Depends()):
    return {'access_token' : form_data.username + 'token'}

@app.get('/')
async def index(token: str = Depends(oauth2_scheme)):  
    return  {'the_token' : token}

"""

@app.get("/data", dependencies=[Depends(verify_api_key)])
def read_ProdTable(
    start_date: date = Query(..., description="Start date in YYYY-MM-DD"),
    end_date: date = Query(..., description="End date in YYYY-MM-DD")
):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT * FROM dbo.ProdTable
        WHERE DATE BETWEEN ? AND ?
    """
    cursor.execute(query, start_date, end_date)
    
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    conn.close()
    return results