from fastapi import FastAPI
from fastapi.responses import JSONResponse
from utils.logger_util import LoggerUtil

from fastapi import File, UploadFile, HTTPException
import shutil
        
import io
import uvicorn


import requests, jsonify
import pandas as pd
import os
from datetime import datetime
import json
import sqlalchemy
import mysql.connector
from decimal import Decimal
import pandas.io.sql as psql
from mysql.connector import Error


database_username = os.environ.get("DB_USER")
database_password = os.environ.get("DB_PASS")
database_ip = os.environ.get("DB_HOST")
database_name = os.environ.get("DB_NAME")

logger = LoggerUtil.get_logger(__name__)

sqlalchemy_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

database_connection = mysql.connector.connect(host=database_ip,
                                         database=database_name,
                                         user=database_username,
                                         password=database_password)
app = FastAPI()
class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Decimal):
        return str(obj)
    return json.JSONEncoder.default(self, obj)

def connection(database_connection):
    if not database_connection.is_connected():
        conn = mysql.connector.connect(host=database_ip,
                                         database=database_name,
                                         user=database_username,
                                         password=database_password)
    else:
        return database_connection
    return conn

def is_iso_date(date_str):
    if pd.isnull(date_str):
        return True
    try:
        datetime.fromisoformat(date_str)
        return True
    except ValueError:
        return False
    
def truncate_table(table):
    try:
        conn = connection(database_connection)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(f"TRUNCATE TABLE {table}")
            conn.commit()
            logger.info(f"TRUNCATE TABLE {table} Success")

    except Error as e:
        logger.error(f"Error: TRUNCATE TABLE {e}")
    finally:
        if database_connection.is_connected():
            cursor.close()
            database_connection.close()

# @app.route('/employees', methods=['POST'])
# def upload_file():
@app.post("/employees")
async def create_upload_file(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        logger.error("File is not a CSV")

        raise HTTPException(status_code=400, detail="File is not a CSV")

    file_path = os.path.join('employees', file.filename)
    # file.save(file_path)
    # data = pd.read_csv(file_path,header=None, names = ["name", "dt_hired", "department_id", "job_id"])

    contents = await file.read()
    data = pd.read_csv(io.BytesIO(contents),header=None, names = ["id","name", "dt_hired", "department_id", "job_id"])
    folder_creation('employees')
    data.to_csv(file_path, index=False)
    logger.info(f"File {file_path} created")


    # data = pd.read_csv(io.BytesIO(contents),header=None, names = ["id","name", "dt_hired", "department_id", "job_id"])
    # data = pd.read_csv(file_path,header=None, names = ["id","name", "dt_hired", "department_id", "job_id"])
    data_validation(data)
    data["department_id"] = data["department_id"].apply(float)
    data["job_id"] = data["job_id"].apply(float)

    truncate_table(f"{database_name}.hired_employees")

    #remove index column 
    del data[data.columns[0]]
    try:

        batches = [data[i:i + 1000] for i in range(0, data.shape[0], 1000)]
        # print ("batches",len(batches))
        for batch in batches:
            batch.to_sql(schema=database_name, name='hired_employees', con=sqlalchemy_connection, if_exists='append', index=False)
    except Exception:
        logger.error("There was an error uploading the file")

        raise HTTPException(status_code=400, detail="There was an error uploading the file")
    logger.info("insert hired_employees success")

    out = data.to_json(orient='records')
    json_object = json.loads(out)

    logger.info("employees Success")

    return JSONResponse(status_code=200, content=json_object)

   
@app.get("/total_employees_per_quarter")
def total_employees_per_quarter():
    db_connection = connection(database_connection)

    
    query = """select department, 
                    job, 
                    IFNULL(Q1, 0) as Q1,
                    IFNULL(Q2, 0) as Q2,
                    IFNULL(Q3, 0) as Q3,
                    IFNULL(Q4, 0) as Q4
                from hired_by_quarter order by department,job ASC
    """
    logger.info(f"Query: {query}")

    cursor = db_connection.cursor()
    cursor.execute(query)
    row_headers=[x[0] for x in cursor.description] #this will extract row headers

    row_headers = ["deparment","job","Q1","Q2","Q3","Q4"]
    records = cursor.fetchall()

    json_data=[]

    for result in records:
        json_data.append(dict(zip(row_headers,result)))
    json_output = json.loads(json.dumps(json_data, cls=DecimalEncoder))

    return JSONResponse(status_code=200, content=json_output)

@app.get("/top_hired_by_department")
def top_hired_by_department():
    db_connection = connection(database_connection)

    query = f"""select count(*)
                from {database_name}.hired_employees where YEAR(dt_hired) = 2021;"""
    logger.info(f"Query: {query}")

    cursor = db_connection.cursor()
    cursor.execute(query)
    total_employees_hired_2021 = cursor.fetchall()

    total_employees_hired_2021 = total_employees_hired_2021[-1][-1]
    query = f"""select count(*)
                from {database_name}.departments """
    logger.info(f"Query: {query}")

    cursor = db_connection.cursor()

    cursor.execute(query)
    total_departments = cursor.fetchall()
    total_departments = total_departments[-1][-1]
    mean=int(total_employees_hired_2021/total_departments)

    logger.info(f"top_hired_by_department: mean {mean}")


    


    query = """ select  d.id ,d.department, count(*) as hired
                from hired_employees a 
                inner join departments d on a.department_id =d.id
                where YEAR(dt_hired) =2021
                group by d.department having count(*) > """ + str(mean) +""" 
                order by 3 desc
            """
    logger.info(f"Query: {query}")

    cursor = db_connection.cursor()
    cursor.execute(query)
    row_headers=[x[0] for x in cursor.description] 

    records = cursor.fetchall()
    # print("Total number of rows in table: ", cursor.rowcount)
    # print("records: ", records)
    json_data=[]
    for result in records:
 
        json_data.append(dict(zip(row_headers,result)))
    # print("json_data ", json_data)
    # total_departments.loc[:, 'weight'].mean()
    # return     json_data, 303 
    return JSONResponse(status_code=200, content=json_data)

def data_validation(data):
  

    if data.shape[1] < 5:
        logger.error("CSV must have at least 5 columns")

        raise HTTPException(status_code=400, detail="CSV must have at least 5 columns")

    if not all(is_iso_date(date) for date in data.iloc[:, 2]):
        logger.error("Column 3 must contain valid ISO date strings")

        raise HTTPException(status_code=400, detail="Column 3 must contain valid ISO date strings")
def folder_creation(folder):
    if not os.path.exists(folder):
        logger.info(f"Folder {folder} created")
        os.makedirs(folder)

def upload_file(folder,df,file_path):
    folder_creation(folder)
    df.to_csv(file_path, index=False)
