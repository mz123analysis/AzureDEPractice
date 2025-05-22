import numpy as np
import pandas as pd
from datetime import date
import random

# We will create sample data for an imaginary hotel, Create Past Dates, 
# Room Sold, Revenue, Competitor Rooms Sold, and Competitor Revenue
#
random.seed(10) # Ensure we get the same numbers

numdays = 1000
todaysDate = date.today()
dates = [todaysDate - pd.Timedelta(days=x) for x in range(numdays)]
    
# We will have multiple hotels. Five imaginary hotels
Initial_Hotel = "Imaginary Hotel "
Hotels = [Initial_Hotel + str(x + 1) for x in range(5)]

dataset = pd.DataFrame(columns=["Hotel","Total Rooms", "Date", "Rooms Solds", "Rooms Revenue","CompSet Total Rooms", "CompSet Rooms Sold", "CompSet Rooms Revenue"])

for hotel in Hotels:
    Staging = pd.DataFrame(columns=["Hotel","Total Rooms", "Date", "Rooms Solds", "Rooms Revenue", "CompSet Total Rooms", "CompSet Rooms Sold", "CompSet Rooms Revenue"])
    
    # Generating random numbers for these specific metrics
    Total_Rooms = random.randint(150, 500)
    Room_Sold = [random.randint(0,Total_Rooms) for _ in range(numdays)]
    Room_Rev = [random.randint(5000, 100000) for _ in range(numdays)]
    Comp_Rooms = random.randint(150, 2000)
    Comp_Room_Sold = [random.randint(0,Comp_Rooms) for _ in range(numdays)]
    Comp_Room_Rev = [random.randint(5000, 250000) for _ in range(numdays)]

    # Applying them to the dataframe
    Staging["Date"]= dates #Date is first so it gives 1000 rows.
    Staging["Total Rooms"] = Total_Rooms
    Staging["Hotel"] = hotel
    Staging["Rooms Solds"] = Room_Sold
    Staging["Rooms Revenue"] = Room_Rev
    Staging["CompSet Total Rooms"] = Comp_Rooms
    Staging["CompSet Rooms Sold"] = Comp_Room_Sold
    Staging["CompSet Rooms Revenue"] = Comp_Room_Rev

    dataset = pd.concat([dataset, Staging], ignore_index=True) # Concatenates new rows

dataset.to_csv("AzureDEPractice\File Creation\Initial_Data.csv")