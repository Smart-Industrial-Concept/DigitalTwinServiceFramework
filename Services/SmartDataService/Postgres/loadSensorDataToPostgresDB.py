#%%
import pandas as pd
import psycopg2

#Database parameters
user='sensorData'
passw='password'
host='localhost'
port='5432'
db='sensorData'


#connect to DB
conn = psycopg2.connect(database=db, user=user, password=passw)

#%%create table
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS sensorReadings(
    index INT,
    timestamp TIMESTAMP PRIMARY KEY,
    T_hx REAL,
    T_sup REAL,
    T_in REAL,
    T_p REAL,
    T_ext REAL,
    m_in REAL,
    m_out REAL,
    Actuator_H1 REAL,
    Actuator_Fan1 REAL,
    Actuator_Fan2 REAL    
)
""")
conn.commit()

#%%fill table with data from the csv-file
cur = conn.cursor()
with open('artificialSensorData.csv', 'r') as f:
    # Notice that we don't need the `csv` module.
    next(f) # Skip the header row.
    cur.copy_from(f, 'sensorReadings', sep=',')

conn.commit()

#%% close db connection
conn.close()