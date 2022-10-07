from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from json import dumps
import pyodbc
server = 'servidor2022.database.windows.net'
database = 'db_test'
username = 'esedanh'
password = 'Esheshesh!'   
driver= '{ODBC Driver 17 for SQL Server}'

app = Flask(__name__)
api = Api(app)


class Metrica1(Resource):
    def get(self):
        conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()
        cursor.execute("select * from metrica1 order by department,job")
        ans = cursor.fetchall()
        return {'department': [i[0] for i in ans],'job': [i[1] for i in ans],'q1': [i[0] for i in ans],'q2': [i[0] for i in ans],'q3': [i[0] for i in ans],'q4': [i[0] for i in ans],'q5': [i[0] for i in ans]} 

class Metrica2(Resource):
    def get(self):
        conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()
        cursor.execute("select * from metrica2 order by hired desc")
        ans = cursor.fetchall()
        return {'id': [i[0] for i in ans],'department': [i[1] for i in ans],'hired': [i[2] for i in ans]} 


api.add_resource(Metrica1, '/result1') 
api.add_resource(Metrica2, '/result2') 

app.run(port='5005')