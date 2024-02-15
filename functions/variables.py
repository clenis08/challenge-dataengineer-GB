##Esquema tipo diccionario de los datos
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

##URL para enviar data de transacciones
url = 'http://127.0.0.1:5000/proccestransactions'

##Folder donde se encuentran los archivos
path = './files/'
