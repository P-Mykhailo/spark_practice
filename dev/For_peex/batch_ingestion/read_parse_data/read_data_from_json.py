import json
import requests
# Opening JSON file
f = open("C:/Users/mpuga/PycharmProjects/spark_practice/dev/data_for_examples/rdo-lookup-data.json")

# returns JSON object as
# a dictionary
data = json.load(f)

# Iterating through the json
# list
for i in data:
    print(type(i))
    print(i)
#Print values from tagID column
for i in data:
    print(i["tagID"])

#Get data from site uses requests
response = requests.get("https://jsonplaceholder.typicode.com/todos")
todos = json.loads(response.text)
print("\n Data from site \n")
#print result as list
print(type(todos))
print(todos)
print()
#print result in dict
for i in todos:
    print(type(i))
    print(i)