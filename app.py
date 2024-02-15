from flask import Flask, render_template
from flask_restful import Resource, Api, reqparse
import pandas as pd
from io import StringIO
from sqlconnection import connectionSQL

filesSchema = {'departments':{'id':'int64',
                            'department':'object'},
                'jobs':{'id':'int64',
                        'job':'object'},
                'hired_employees':{'id':'int64',
                                  'Name':'object',
                                  'datetime':'object',
                                  'department_id':'float64',
                                  'job_id':'float64'},

                }
app = Flask(__name__) ## creando servidor API
api = Api(app)

quarter_departments = '''
SELECT 
    department
    ,job
    ,CASE WHEN Q=1 THEN qty else 0 end as Q1
    ,CASE WHEN Q=2 THEN qty else 0 end as Q2
    ,CASE WHEN Q=3 THEN qty else 0 end as Q3
    ,CASE WHEN Q=4 THEN qty else 0 end as Q4
FROM (
	SELECT distinct
		d.department AS department
        ,j.job AS job
		,DATEPART(QUARTER, datetime) as Q
		,COUNT(*) AS qty
	FROM dbo.hired_employees e
	inner join dbo.jobs j
		ON e.job_id = j.id
	inner join dbo.departments d
		ON e.department_id = d.id
	WHERE year(e.datetime) = 2021
    GROUP BY job, department, DATEPART(QUARTER, datetime)
) AS basequery ORDER BY department
'''

mean_departments = '''
WITH base_query AS (

 SELECT distinct
        d.id as id
		,d.department AS department
		,COUNT(name) AS qty
	FROM dbo.hired_employees e
	inner join dbo.departments d
		ON e.department_id = d.id
    WHERE year(e.datetime) = 2021
    GROUP BY d.id, d.department
)
SELECT  *
from base_query
where qty > (select avg(qty) from base_query)
'''

@app.route('/quarterDepartments')
def quarterDepartments():

    cursor,cnxn,engine=connectionSQL()
    
    ## Enviado query
    df = pd.read_sql(quarter_departments, cnxn)

    cnxn.commit() 
    cursor.close()

    # Data presentation
    return render_template('index.html',  tables=[df.to_html(classes='data')], titles=df.columns.values)

# end-point to obtain number of employees hired of each department that hired more
# employees than the mean of employees hired in 2021 for all the departments.
@app.route('/meanDepartmens')
def meanDepartments():

    cursor,cnxn,engine=connectionSQL()
    
    ## Enviado query
    df = pd.read_sql(mean_departments, cnxn)

    cnxn.commit() 
    cursor.close()

    return render_template('index.html',  tables=[df.to_html(classes='data')], titles=df.columns.values)

class proccestransactions(Resource):
        
    def post(self):        
        # Params required in the post request
        parser = reqparse.RequestParser()
        parser.add_argument('fileName', required=True, type=str)
        parser.add_argument('fileData', required=True, type=str)

        args = parser.parse_args()

        fileName = args['fileName']
        fileData = args['fileData']
        
        try:
            # Retreiving and transforming data into a pandas dataframe
            data = StringIO(fileData)
            df=pd.read_csv(data)

            # Data cleansing
            df = df.dropna()
            
            # Data schema must be validated to avoid errors during insert into SQL DB
            expected_dtypes = list(filesSchema[fileName.lower()].values())
            input_dtypes = [str(list(df.dtypes)[i]) for i in range(len(list(df.dtypes)))]

            if (expected_dtypes != input_dtypes):
                return {'message': "Esquema incompatible con la tabla de destino"}, 400
            
        except:
            return {'message': "Archivo no valido."}, 500
        
        if len(df) > 1000:
            return {'message':'El archivo supera las 1000 filas.'}, 200
        
        ## Conecta a sql database
        print(f"insertar datos en tabla {fileName}")
        cursor,cnxn,engine=connectionSQL()
        ## inserta los datos en la tabla 
        df.to_sql(fileName, engine, schema='dbo', if_exists='append', index=False)
        cursor.commit()
        cnxn.close()
        return {'message': "Datos CSV procesados correctamete."}, 200

api.add_resource(proccestransactions, '/proccestransactions')

if __name__ == "__main__":
    app.run(debug=True)