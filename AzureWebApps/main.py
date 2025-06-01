from fastapi import FastAPI, Depends, Query
from db import get_connection
from security import verify_api_key
from datetime import date

app = FastAPI()

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