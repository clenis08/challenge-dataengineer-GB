# Data Challenge
This project is big data migration to a new database system. 

## Description 

This project consist in a local RESTAPI development in python with flask. With this service, the user can migrate hystorical data from csv files to a SQL database.

With this API we can migrate data from three files:

*hired_employees.csv
*departmenes.csv
*jobs.csv

Also the procces of migrate transactions is doing in batch without exceeding the 1000 records.

In other hand with the API we can consult the SQL Database and ask for information, in this case we have two querys define:

*quarter_departments (Obtain the number of employees hired for department and quarter)
*mean_departments (Obtain the list of departmenst that have hired employees over the mean)

## 📑 Technologies uses:
To development the API we use the next tecnhologies:
* Python 3.0
* Flask
* Pandas
* flask_restful
* pyodbc
* Git

To host de database servie we use Azure SQL Database.

*Server endpoint: sql-server-datachallenge.database.windows.net
*SQL Database: azdbsqlchallenge



**Cali - Colombia** 

## License
[MIT](https://choosealicense.com/licenses/mit/)