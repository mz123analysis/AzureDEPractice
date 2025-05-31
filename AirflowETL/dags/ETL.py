from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
import pandas as pd
from datetime import date, datetime
import pyodbc

DAG_NAME = "ETLDatabase"
SCHEDULE_INTERVAL = Variable.get("Schedule: DailyETL", default_var = '0 5 * * *')
DAG_TAGS=['V1.0', 'ETL']

with DAG(DAG_NAME,
        description='ETL',
        schedule_interval= SCHEDULE_INTERVAL,
        start_date=datetime(2025, 5, 28),
        catchup=False,
        tags=DAG_TAGS,
        ) as dag:

    # Better to create a connection and do a task, instead of sharing one connection.
    @task.branch
    def Check_Data_Exists():

        conn = pyodbc.connect('Encrypt=Optional;DRIVER={ODBC Driver 18 for SQL Server}; '
        ' SERVER=' + Variable.get("Azureserver", deserialize_json=True) +'; DATABASE='+ Variable.get("AzureDB", deserialize_json=True)+
        ';UID=' + Variable.get("Azure_User", deserialize_json=True) +';PWD=' + Variable.get("MSSQL_Pass", deserialize_json=True))
        cursor = conn.cursor()

        cursor.execute("TRUNCATE TABLE dbo.Staging")
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM dbo.ProdTable")
        rows = cursor.fetchone()[0] # [0] gets the value from fetchone
    
        cursor.close()
        conn.close()
        
        if rows == 0: # Not necessary to do branching for this task, but can be extremely useful in the real world.
            return "initial_data_task"
        return "daily_task"

    @task
    def initial_data_task():
        dataset = pd.read_csv("/opt/airflow/data/Initial_Data.csv")
        inputs = [tuple(row) for row in dataset.values]

        SQL_Statement = '''
            INSERT INTO [dbo].[Staging] (Date, Hotel, Rooms, RoomsSold, RoomsRevenue,
            CompSetRooms, CompSetRoomsSold, CompSetRoomsRevenue) VALUES
            (?,?,?,?,?,?,?,?)
        '''
        conn = pyodbc.connect('Encrypt=Optional;DRIVER={ODBC Driver 18 for SQL Server}; '
        ' SERVER=' + Variable.get("Azureserver", deserialize_json=True) +'; DATABASE='+ Variable.get("AzureDB", deserialize_json=True)+
        ';UID=' + Variable.get("Azure_User", deserialize_json=True) +';PWD=' + Variable.get("MSSQL_Pass", deserialize_json=True))
        
        cursor = conn.cursor()

        cursor.executemany(SQL_Statement,inputs)
        conn.commit()

        cursor.close()
        conn.close()
        
    @task
    def daily_task():

        dataset = pd.read_csv("/opt/airflow/data/Daily.csv")
        inputs = [tuple(row) for row in dataset.values]

        SQL_Statement = '''
            INSERT INTO [dbo].[Staging] (Date, Hotel, Rooms, RoomsSold, RoomsRevenue,
            CompSetRooms, CompSetRoomsSold, CompSetRoomsRevenue) VALUES
            (?,?,?,?,?,?,?,?)
        '''

        conn = pyodbc.connect('Encrypt=Optional;DRIVER={ODBC Driver 18 for SQL Server}; '
        ' SERVER=' + Variable.get("Azureserver", deserialize_json=True) +'; DATABASE='+ Variable.get("AzureDB", deserialize_json=True)+
        ';UID=' + Variable.get("Azure_User", deserialize_json=True) +';PWD=' + Variable.get("MSSQL_Pass", deserialize_json=True))
        cursor = conn.cursor()

        cursor.executemany(SQL_Statement,inputs)
        conn.commit()

        cursor.close()
        conn.close()
    
    @task(trigger_rule='one_success')
    def Staging_to_Prod():
         
        conn = pyodbc.connect('Encrypt=Optional;DRIVER={ODBC Driver 18 for SQL Server}; '
        ' SERVER=' + Variable.get("Azureserver", deserialize_json=True) +'; DATABASE='+ Variable.get("AzureDB", deserialize_json=True)+
        ';UID=' + Variable.get("Azure_User", deserialize_json=True) +';PWD=' + Variable.get("MSSQL_Pass", deserialize_json=True))
        cursor = conn.cursor()

        SQL_Statement = """
        EXECUTE [dbo].[SP_Staging_to_Prod];
        """

        cursor.execute(SQL_Statement)
        conn.commit()

        cursor.close()
        conn.close()

    @task
    def reset_Staging():
        conn = pyodbc.connect('Encrypt=Optional;DRIVER={ODBC Driver 18 for SQL Server}; '
        ' SERVER=' + Variable.get("Azureserver", deserialize_json=True) +'; DATABASE='+ Variable.get("AzureDB", deserialize_json=True)+
        ';UID=' + Variable.get("Azure_User", deserialize_json=True) +';PWD=' + Variable.get("MSSQL_Pass", deserialize_json=True))
        cursor = conn.cursor()

        cursor.execute("TRUNCATE TABLE dbo.Staging")
        conn.commit()
        
        cursor.close()
        conn.close()

    # The Branching chooses which dataset to utilize
    Check_Data_Exists() >> [initial_data_task(), daily_task()] >> Staging_to_Prod() >> reset_Staging()