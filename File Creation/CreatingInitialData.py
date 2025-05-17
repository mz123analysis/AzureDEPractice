import numpy as np
import pandas as pd
from datetime import date

# We will create sample data for an imaginary hotel, Create Past Dates, 
# Room Sold, Revenue, Competitor Rooms Sold, and Competitor Revenue
#

numdays = 1000
todaysDate = date.today()
dates = [todaysDate - pd.Timedelta(days=x) for x in range(numdays)]

for x in dates:
    print(x)
    
# We will have multiple hotels
Initial_Hotel = "Imaginary Hotel"
