from sqlalchemy import create_engine
import pyodbc

def connectionSQL():
    server ='sql-server-datachallenge.database.windows.net'
    database ='azdbsqlchallenge'
    username ='sqladmin'
    password ='5:3Bf93GwJ/|'
    driver = '{ODBC Driver 17 for SQL Server}'
    conn_str = 'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    
    try:
        cnxn = pyodbc.connect(conn_str)
        # cursor = cnxn.cursor()
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str}')
        return cnxn.cursor(),cnxn,engine
        
    except Exception as e:
        print(e)
        print(f'Cannot connect to SQL server', {e})