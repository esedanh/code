import requests
import pandas as pd
import json
import datetime
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from io import StringIO    
import pyodbc

def rest_api():
    api_url1 = "https://newstorage22.blob.core.windows.net/transactions/department1.json"
    response1 = requests.get(api_url1)
    r1=response1.json()

    api_url2 = "https://newstorage22.blob.core.windows.net/transactions/job1.json"
    response2 = requests.get(api_url2)
    r2=response2.json()

    api_url3 = "https://newstorage22.blob.core.windows.net/transactions/employee1.json"
    response3 = requests.get(api_url3)
    r3=response3.json()

    df1 = pd.DataFrame(r1, columns = ['id','department'])
    df2 = pd.DataFrame(r2, columns = ['id','job'] )
    df3 = pd.DataFrame(r3, columns = ['id','name','datetime','department_id','job_id'])

    return df1,df2,df3


def carga_informacion_val(df1,df2,df3,server,database,driver,username,password):

    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()

    for index, row in df2.iterrows():
        cursor.execute("INSERT INTO jobs_log (id,job) values(?,?)", row.id, row.job)
        if row.id!="" and row.id!="0" and row.job!="":
            cursor.execute("INSERT INTO jobs (id,job) values(?,?)", row.id, row.job)
    conn.commit()

    for index, row in df3.iterrows():
        cursor.execute("INSERT INTO hired_employees_log (id,name,datetime,department_id,job_id) values(?,?,?,?,?)", row.id, row.name,row.datetime,row.department_id,row.job_id)
        if row.id!="":
            cursor.execute("INSERT INTO hired_employees (id,name,datetime,department_id,job_id) values(?,?,?,?,?)", row.id, row.name,row.datetime,row.department_id,row.job_id)
    conn.commit()

    for index, row in df1.iterrows():
        cursor.execute("INSERT INTO department_log (id,department) values(?,?)", row.id, row.department)
        if row.id!="" and row.id!="0" and row.department!="": 
            cursor.execute("INSERT INTO department (id,department) values(?,?)", row.id, row.department)
    conn.commit()