import mysql.connector
import pandas as pd


def ensure_database(db_config):
    temp_config = db_config.copy()
    temp_config.pop('database')
    conn = mysql.connector.connect(**temp_config)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_config['database']}")
    cursor.close()
    conn.close()

def load_data(file, db_config):
    ensure_database(db_config)
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    dataset = pd.read_csv(file, parse_dates=['timestamp'])
    try:
        cursor.execute("SELECT count(*) FROM events;")
        table_len = cursor.fetchone()[0]
    except Exception:
        table_len = 0
    if len(dataset) == 0:
        print("The dataset is empty.")
    elif dataset.isnull().values.any():
        print("The dataset contains null values.")
    elif table_len == len(dataset):
        print("The dataset is already loaded in the database.")
    else:
        print("Loading data into the database...")
        # Replace table using pandas to_sql with SQLAlchemy engine
        dataset.to_sql('events', engine, if_exists='replace', index=False)
    cursor.close()
    conn.close()

def createL47(db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("create table if not exists Line47 "
                  "(start_timestamp datetime,"
                   "end_timestamp datetime,"
                   "duration time)" )
    
def queryExecution(query,db_config):
    conn = mysql.connector.connect(**db_config)
    df = pd.read_sql(query,conn)
    conn.close()
    return df
    

db_config = {
    'user': 'runtheorun',
    'password': '9TDidJ0w',
    'host': 'localhost',
    'database': 'witside'
}

from sqlalchemy import create_engine
engine = create_engine(
f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
)

# load_data('dataset.csv', db_config)

# createL47(db_config)
line47Events = queryExecution("select * from events where production_line_id like '%47' and STATUS <> 'ON';", db_config)
print(line47Events)