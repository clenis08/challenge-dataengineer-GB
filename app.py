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