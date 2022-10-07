import challenge
import challenge_p2
import backup_ope
from datetime import date
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=newstorage22;AccountKey=HZlYKM0byswwB5LKufYuWelRF2YaGoksUSF8dzY97SpzpFBwpSrEc+Es+LneOIy72stXiN3V2zvR+AStODD32A==;EndpointSuffix=core.windows.net"
CONTAINERNAME = "historicos"
BLOBNAME1 = "jobs.csv"
BLOBNAME2 = "departments.csv"
BLOBNAME3 = "hired_employees.csv"
CONTAINERBKP = "bkp"
server = 'servidor2022.database.windows.net'
database = 'db_test'
username = 'esedanh'
password = 'Esheshesh!'   
driver= '{ODBC Driver 17 for SQL Server}'
today = date.today()

bkp_job = "bkp_job_"+str(today) + ".csv" 
bkp_dep = "bkp_department_"+str(today) + ".csv" 
bkp_emp = "bkp_employee_"+str(today) + ".csv" 

df_job,df_dep,df_emp = challenge.lectura_informacion(CONNECTION_STRING,CONTAINERNAME,BLOBNAME1,BLOBNAME2,BLOBNAME3)
challenge.carga_informacion(df_job,df_dep,df_emp,server,database,driver,username,password)

df1_dep,df2_job,df3_emp = challenge_p2.rest_api()
challenge_p2.carga_informacion_val(df1_dep,df2_job,df3_emp,server,database,driver,username,password)

df_dep_select = backup_ope.select_backup1(server,database,username,password,driver)
df_job_select = backup_ope.select_backup2(server,database,username,password,driver)
df_emp_select = backup_ope.select_backup3(server,database,username,password,driver)

backup_ope.crear_backup(CONTAINERBKP,bkp_job,df_job_select,CONNECTION_STRING)
backup_ope.crear_backup(CONTAINERBKP,bkp_dep,df_dep_select,CONNECTION_STRING)
backup_ope.crear_backup(CONTAINERBKP,bkp_emp,df_emp_select,CONNECTION_STRING)
backup_ope.generaciontabla1(server,database,driver,username,password)
backup_ope.generaciontabla2(server,database,driver,username,password)
backup_ope.generaciontabla3(server,database,driver,username,password)

df_jobr,df_depr,df_empr = backup_ope.lectura_informacion(CONNECTION_STRING,CONTAINERBKP,bkp_job,bkp_dep,bkp_emp)
backup_ope.carga_informacion(df_jobr,df_depr,df_empr,server,database,driver,username,password)