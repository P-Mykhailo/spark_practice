import pyodbc
#Connection to my local database
connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
"Server=5CD9126FQP\SQLEXPRESS;"
"Database=my_database;"
"Trusted_Connection=yes;")
#SQL query for extract data from database table
query = """
            SELECT TOP (10) [intervalidentifier]
                            ,[eventtypeid]
                            ,[message_orig]
                            ,[message_revised]
                            ,[priority_orig]
                            ,[priority_revised] 
            FROM mem_alert_new_table
            WHERE intervalidentifier = '__AlarmValue' AND message_orig LIKE '%03%'
            
"""
cursor = connection.cursor()
cursor.execute(query)
# print results
for row in cursor:
    print(row)