import pandas as pd
import requests ## permite enviar peticiones HTTP request using python
import json
from variables import * 
url = 'http://127.0.0.1:5000/proccestransactions'

path = './files/'

for file, schema in filesSchema.items():
    print(list(schema.keys()))
    df = pd.read_csv(f'{path}{file}.csv', header=None) #Leo el csv lo convierto en df
    df.columns = list(schema.keys()) ## asigna los valores de del esquema definido al dataframe

    # determino la cantidad de paquetes a enviar, capturando la cantidad de paquetes
    qt = len(df)//1000 # division entera

    if len(df) % 1000 > 0: # si es mas de 1000
        qt += 1 # aumento los paquetes a enviar
    
    # Sending packages
    for pack in range(qt):
        initPos = pack*1000

        # In the last packet only the remaining data should be sent
        finPos = initPos + len(df) % 1000 if pack == qt else pack*1000 + 1000

        df_to_Send = df.iloc[initPos:finPos,:]
        csvFile = df_to_Send.to_csv(index=False)
        print(file)
        datos = {"fileName": file,
                 "fileData": csvFile}

        # Request
        respuesta = requests.post(url, data=json.dumps(datos), headers = {"Content-Type": "application/json"})
        
        if respuesta.status_code == 200:
            print('Datos enviados correctamente.')
        else:
            print('Error al enviar los datos. Código de estado:', respuesta.status_code)