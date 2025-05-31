from airflow import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.models import Variable
from airflow.decorators import task
import pandas as pd
from datetime import date, datetime
import random

DAG_NAME = "PushingDailyData"
SCHEDULE_INTERVAL = Variable.get("Schedule: Weekly-STR", default_var = '0 5 * * *')
DAG_TAGS=['V1.0', 'SFTP']

with DAG(DAG_NAME,
        description='Dailt Data Import',
        schedule_interval= SCHEDULE_INTERVAL,
        start_date=datetime(2025, 5, 24),
        catchup=False,
        tags=DAG_TAGS,
        ) as dag:

    @task
    def daily_data():
        random.seed(10) 

        Hotels = {
            "Imaginary Hotel 1" : 442,
            "Imaginary Hotel 2" : 335,
            "Imaginary Hotel 3" : 156,
            "Imaginary Hotel 4" : 490,
            "Imaginary Hotel 5" : 311
        }

        CS_Hotels = {
            "Imaginary Hotel 1" : 252,
            "Imaginary Hotel 2" : 533,
            "Imaginary Hotel 3" : 978,
            "Imaginary Hotel 4" : 1130,
            "Imaginary Hotel 5" : 318
        }
            
        #Final Dataset    
        rows = []
        for hotel, rooms in Hotels.items():

            # Generating random numbers for these specific metrics
            Total_Rooms = rooms
            Room_Sold = random.randint(1,Total_Rooms)
            Room_Rev = random.randint(5000, 100000)
            Comp_Rooms = CS_Hotels[hotel]
            Comp_Room_Sold = random.randint(1,Comp_Rooms)
            Comp_Room_Rev = random.randint(5000, 250000)

            row = {
            "Date" : date.today(),
            "Hotel" : hotel,
            "Total Rooms" : Total_Rooms,
            "Rooms Solds" : Room_Sold,
            "Rooms Revenue" : Room_Rev,
            "CompSet Total Rooms" : Comp_Rooms,
            "CompSet Rooms Sold" : Comp_Room_Sold,
            "CompSet Rooms Revenue" : Comp_Room_Rev
            }

            rows.append(row) # Concatenates new rows

        dataset = pd.DataFrame(rows)
        dataset.to_csv("/opt/airflow/data/Daily.csv",index=False)

    @task
    def Daily_Push_To_SFTP():   
    
        upload_to_sftp = SFTPOperator(
            task_id='upload_Daily_to_sftp',
            ssh_conn_id= 'Azure_SFTP',
            local_filepath='/opt/airflow/data/Daily.csv',
            remote_filepath='/Daily.csv',
            operation="put",
        )
        upload_to_sftp.execute(context={})

    daily_data() >> Daily_Push_To_SFTP()