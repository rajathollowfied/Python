import logging
import pandas as pd
from io import StringIO
import json
import psycopg2

logging.basicConfig(filename='logs\etl.log', level=logging.INFO)

def configLoad():
    with open('D:\Learning\GIT\Python//footballETL\config\dbConfig.json', 'r') as configFile:
        dbConfig = json.load(configFile)
    return dbConfig

def etlConfigLoad():
    with open('D:\Learning\GIT\Python//footballETL\config\etlConfig.json', 'r') as configFile:
        etlConfig = json.load(configFile)
    return etlConfig

def dbConnection():
    dbConfig = configLoad()
    try:
        connection = psycopg2.connect(
            host = dbConfig['database']['DB_HOST'],
            port = dbConfig['database']['DB_PORT'],
            database = dbConfig['database']['DB_NAME'],
            user = dbConfig['database']['DB_USER'],
            password = dbConfig['database']['DB_PASSWORD']
        )
        cursor = connection.cursor()
        logging.info("Connection successful!")
        
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        logging.info(f"PostgreSQL version:{record}")

        return cursor,connection

    except Exception as e:
        logging.error(f"Error while connecting to DB: {e}")

def fileSchemas(fileType):
    try:
        if(fileType == 0):
            schema = {
            'date': str,
            'home_team': str,
            'away_team': str,
            'team': str,
            'scorer': str,
            'minute': int,
            'own_goal': bool,
            'penalty':  bool
            }
        elif(fileType == 1):
            schema = {
            'date': str,
            'home_team': str,
            'away_team': str,
            'team': str,
            'home_score': int,
            'away_score' : int,
            'tournament' : str,
            'city' : str,
            'countrty' : str,
            'neutral':  bool
            }
        elif(fileType == 2):
            schema = {
            'date': str,
            'home_team': str,
            'away_team': str,
            'winner' : str,
            'shooter' : str
            }
        return schema
    
    except Exception as e:
        logging.error(f"some issue in selecting schema probably: {e}")
            
def readCSV_to_pgDB(cursor,connection,table,path,loopValue):
    try:        
        
        df = pd.read_csv(f'{path}{table}.csv',dtype=fileSchemas(loopValue),header=None)
        df = df.apply(lambda col: col.str.replace(r',(?![^"]*")', '', regex=True).str.strip() if col.dtype == 'object' else col)
        dfToLoad = df.drop(index=0)
        
        buffer = StringIO()
        dfToLoad.to_csv(buffer, index=False, header=False)
        
        buffer.seek(0)
        
        cursor.copy_from(buffer,table.lower(),sep=',',null='')
        connection.commit()
        logging.info(f"Data loaded into {table.lower()} successfully!")

    except Exception as e:
        logging.error(f"Error, rollingback: {e}")
        connection.rollback()
        


def main():
    
    cursor, connection = dbConnection()
    etlConfig = etlConfigLoad()
    path = etlConfig['fileInfo']['path']
    for i in range(3):
        table = etlConfig['fileInfo'][f'{i}']
        readCSV_to_pgDB(cursor,connection,table,path,i)
        
        
    #df = pd.read_csv('D:\Learning\GIT\datasets//football\Penalty_Shootouts.csv',dtype=fileSchemas(2),header=None)
    #df[6] = df[6].str.replace(r',(?![^"]*")', '', regex=True).str.strip()
    cursor.close()
    connection.close()
    logging.info(f"Connection closed successfully!")

if __name__ == '__main__':
    main()