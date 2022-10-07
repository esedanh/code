import pyodbc
import pandas as pd
from io import StringIO  
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

def select_backup1 (server,database,username,password,driver):
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.execute("select * from department")
    ans = cursor.fetchall()
    list1 = []
    list2 = []
    for i in ans:
        list1.append(i[0])
        list2.append(i[1])

    cons = list(zip(list1, list2))
    df = pd.DataFrame(cons,columns = ['id','department'])
    return df

def select_backup2 (server,database,username,password,driver):
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.execute("select * from jobs")
    ans = cursor.fetchall()
    list1 = []
    list2 = []
    for i in ans:
        list1.append(i[0])
        list2.append(i[1])

    cons = list(zip(list1, list2))
    df = pd.DataFrame(cons,columns = ['id','job'])
    return df

def select_backup3 (server,database,username,password,driver):
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.execute("select * from hired_employees")
    ans = cursor.fetchall()
    list1 = []
    list2 = []
    list3 = []
    list4 = []
    list5 = []
    for i in ans:
        list1.append(i[0])
        list2.append(i[1])
        list3.append(i[2])
        list4.append(i[3])
        list5.append(i[4])        

    cons = list(zip(list1, list2,list3,list4,list5))
    df = pd.DataFrame(cons,columns = ['id','name','datetime','department_id','job_id'])
    return df

def crear_backup(container,filename,df,connect_str):
    if all([container, len(df), filename]):
        upload_file_path = f"{filename}"
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(
            container=container, blob=upload_file_path
        )
        try:
            output = df.to_csv(index=False, encoding="utf-8")
        except Exception as e:
            pass
        try:
            blob_client.upload_blob(output, blob_type="BlockBlob")
        except Exception as e:
            pass

def generaciontabla1(server,database,driver,username,password):
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE hired_employees1 "
                    "(id int, "
                    "name varchar(100),"
                    "datetime varchar(100),"
                    "department_id int,"
                    "job_id int)")
    conn.commit()

def generaciontabla2(server,database,driver,username,password):
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE jobs1 "
                    "(id int, "
                    "job varchar(100))")
    conn.commit()

def generaciontabla3(server,database,driver,username,password):
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE department1 "
                    "(id int, "
                    "department varchar(100))")
    conn.commit()

def lectura_informacion(CONNECTION_STRING,CONTAINERNAME,BLOBNAME1,BLOBNAME2,BLOBNAME3):

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING) #instantiate new blobservice with connection string
    container_client = blob_service_client.get_container_client(CONTAINERNAME) #instantiate new containerclient
    blob_client1 = blob_service_client.get_blob_client(container = CONTAINERNAME, blob=BLOBNAME1)
    blob_client2 = blob_service_client.get_blob_client(container = CONTAINERNAME, blob=BLOBNAME2)
    blob_client3 = blob_service_client.get_blob_client(container = CONTAINERNAME, blob=BLOBNAME3)

    downloaded_blob1 = container_client.download_blob(BLOBNAME1)
    downloaded_blob2 = container_client.download_blob(BLOBNAME2)
    downloaded_blob3 = container_client.download_blob(BLOBNAME3)

    df_job = pd.read_csv(StringIO(downloaded_blob1.content_as_text()))
    df_dep = pd.read_csv(StringIO(downloaded_blob2.content_as_text()))
    df_emp = pd.read_csv(StringIO(downloaded_blob3.content_as_text()))
    df_emp = df_emp.fillna('')

    return df_job,df_dep,df_emp

def carga_informacion(df_job,df_dep,df_emp,server,database,driver,username,password):
    
    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()

    for index, row in df_job.iterrows():
        cursor.execute("INSERT INTO jobs1 (id,job) values(?,?)", row.id, row.job)
    conn.commit()

    for index, row in df_emp.iterrows():
        cursor.execute("INSERT INTO hired_employees1 (id,name,datetime,department_id,job_id) values(?,?,?,?,?)", row.id, row.name,row.datetime,row.department_id,row.job_id)
    conn.commit()

    for index, row in df_dep.iterrows():
        cursor.execute("INSERT INTO department1 (id,department) values(?,?)", row.id, row.department)
    conn.commit()