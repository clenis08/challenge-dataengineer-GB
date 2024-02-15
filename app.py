from flask import Flask, render_template
from flask_restful import Resource, Api, reqparse
import pandas as pd
from io import StringIO
from functions.sqlconnection import connectionSQL
from functions.variables import filesSchema
from functions.querys import mean_departments, quarter_departments

app = Flask(__name__) ## creando servidor API
api = Api(app)

# end-point para obtener el numero de empleados contratados por cada departamento por Q en 2021
@app.route('/quarterDepartments')
def quarterDepartments():

    cursor,cnxn,engine=connectionSQL()
    
    ## Enviado query
    df = pd.read_sql(quarter_departments, cnxn)

    cnxn.commit() 
    cursor.close()

    # Data presentation
    return render_template('index.html',  tables=[df.to_html(classes='data')], titles=df.columns.values)


# end point para obtener los departamentos que superaron el promedio de contratación en 2021
@app.route('/meanDepartments')
def meanDepartments():

    cursor,cnxn,engine=connectionSQL()
    
    ## Enviado query
    df = pd.read_sql(mean_departments, cnxn)

    cnxn.commit() 
    cursor.close()

    return render_template('index.html',  tables=[df.to_html(classes='data')], titles=df.columns.values)

class proccestransactions(Resource):
        
    def post(self):        
        # Parametros requerido en la peticion post
        parser = reqparse.RequestParser()
        parser.add_argument('fileName', required=True, type=str)
        parser.add_argument('fileData', required=True, type=str)

        args = parser.parse_args()

        fileName = args['fileName']
        fileData = args['fileData']
        
        try:
            # Recibo datos y convierto en un df
            data = StringIO(fileData)
            df=pd.read_csv(data)

            #limpio
            df = df.dropna()
            
            # Validación del esquema de datos
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

api.add_resource(proccestransactions, '/proccestransactions') ## agrego el endpoint para procesar transactiones

if __name__ == "__main__":
    app.run(debug=True)