from flask import Flask, render_template
from flask_restful import Resource, Api, reqparse
from io import StringIO
from datetime import datetime

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

            expected_columns = list(filesSchema[fileName.lower()].keys())
            input_columns = list(df.columns)

            if (expected_dtypes != input_dtypes) or (expected_columns != input_columns):
                return {'message': "Esquema incompatible con la tabla de destino"}, 400
            
        except:
            return {'message': "Archivo no valido."}, 500

        if len(df) > 1000:
            return {'message':'El archivo supera las 1000 filas.'}, 400
        
       

api.add_resource(proccestransactions, '/proccestransactions')

if __name__ == "__main__":
    app.run(debug=True)