import pandas as pd
import numpy as np
import datetime
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from io import StringIO    
import pyodbc

def lectura_informacion(CONNECTION_STRING,CONTAINERNAME,BLOBNAME1,BLOBNAME2,BLOBNAME3):
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING) #instantiate new blobservice with connection string
    container_client = blob_service_client.get_container_client(CONTAINERNAME) #instantiate new containerclient
    blob_client1 = blob_service_client.get_blob_client(container = CONTAINERNAME, blob=BLOBNAME1)
    blob_client2 = blob_service_client.get_blob_client(container = CONTAINERNAME, blob=BLOBNAME2)
    blob_client3 = blob_service_client.get_blob_client(container = CONTAINERNAME, blob=BLOBNAME3)

    downloaded_blob1 = container_client.download_blob(BLOBNAME1)
    downloaded_blob2 = container_client.download_blob(BLOBNAME2)
    downloaded_blob3 = container_client.download_blob(BLOBNAME3)

    df_job = pd.read_csv(StringIO(downloaded_blob1.content_as_text()),names = ["id","job"])
    df_dep = pd.read_csv(StringIO(downloaded_blob2.content_as_text()),names = ["id","department"])
    df_emp = pd.read_csv(StringIO(downloaded_blob3.content_as_text()),names = ["id","name","datetime","department_id","job_id"])
    df_emp = df_emp.fillna('')

    return df_job,df_dep,df_emp


def carga_informacion(df_job,df_dep,df_emp,server,database,driver,username,password):
    
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()

    for index, row in df_job.iterrows():
        cursor.execute("INSERT INTO jobs (id,job) values(?,?)", row.id, row.job)
    conn.commit()

    for index, row in df_emp.iterrows():
        cursor.execute("INSERT INTO hired_employees (id,name,datetime,department_id,job_id) values(?,?,?,?,?)", row.id, row.name,row.datetime,row.department_id,row.job_id)
    conn.commit()

    for index, row in df_dep.iterrows():
        cursor.execute("INSERT INTO department (id,department) values(?,?)", row.id, row.department)
    conn.commit()

