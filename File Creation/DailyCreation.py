import numpy as np
import pandas as pd
from datetime import date
import random

# We have a list of Hotels and number of rooms

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
    Room_Sold = random.randint(0,Total_Rooms)
    Room_Rev = random.randint(5000, 100000)
    Comp_Rooms = CS_Hotels[hotel]
    Comp_Room_Sold = random.randint(0,Comp_Rooms)
    Comp_Room_Rev = random.randint(5000, 250000)

    row = {
    "Date" : date.today(),
    "Total Rooms" : Total_Rooms,
    "Hotel" : hotel,
    "Rooms Solds" : Room_Sold,
    "Rooms Revenue" : Room_Rev,
    "CompSet Total Rooms" : Comp_Rooms,
    "CompSet Rooms Sold" : Comp_Room_Sold,
    "CompSet Rooms Revenue" : Comp_Room_Rev
    }

    rows.append(row) # Concatenates new rows

dataset = pd.DataFrame(rows)
dataset.to_csv("AzureDEPractice\File Creation\Daily.csv")