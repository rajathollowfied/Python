import json
import psycopg2
import logging

logging.basicConfig(filename='logs\etl.log', level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

def etlConfigLoad():
    with open('D:\Learning\GIT\Python//footballETL\config\etlConfig.json', 'r') as configFile:
        etlConfig = json.load(configFile)
    return etlConfig

def configLoad():
    with open('D:\Learning\GIT\Python//footballETL\config\dbConfig.json', 'r') as configFile:
        dbConfig = json.load(configFile)
    return dbConfig

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
        cursor.execute("SELECT VERSION();")
        record = cursor.fetchone()
        logging.info(f"PostgreSQL version:{record}")
        
        return cursor, connection
    
    except Exception as e:
        logging.error(f'connection messed up!{e}')
        
def dbTruncate():
    cursor, connection = dbConnection()
    etlConfig = etlConfigLoad()
    for i in range(3):
        table = etlConfig['fileInfo'][f'{i}']
        
        cursor.execute(f"TRUNCATE TABLE {table}")
        connection.commit()
        logging.info(f"table trunc complete {table}")
    cursor.close()
    connection.close()

dbTruncate()