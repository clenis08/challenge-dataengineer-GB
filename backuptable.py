import pandas as pd
import fastavro
from variables import path
from sqlconnection import connectionSQL
from datetime import datetime



current_date = datetime.now().strftime("%Y%m%d")

tablename='dbo.hired_employees'

query = f'''
select * from {tablename}
'''

def main():

    cursor,cnxn,engine=connectionSQL()
    
    ## Enviado query
    df = pd.read_sql(query, engine)
    cnxn.commit() 
    cursor.close()

    avro_schema = {
    "type": "record",
    "name": "DataFrameRecord",
    "fields": [{"name": col, "type": "string"} for col in df.columns]
    }

    # Convert the DataFrame to a list of records
    records = df.to_dict(orient='records')

    # Specify the output Avro file path
    avro_file_path = path + f"{tablename}_backup_{current_date}.avro"

    # Write the Avro file
    with open(avro_file_path, "wb") as avro_file:
        fastavro.writer(avro_file, schema=avro_schema, records=records)


if __name__ == '__main__':
    main()