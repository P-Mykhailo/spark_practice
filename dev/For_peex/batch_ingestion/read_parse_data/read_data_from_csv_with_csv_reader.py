import csv
import re

with open("/dev/data_for_examples/mem_alert_new_table.csv") as file:
#Read csv file with option delimeter
    csv_dictreader = csv.DictReader(file, dialect='excel', delimiter=",")

    for data in csv_dictreader:
        print(data)
# If we have bad data in priority_orig we write it as None
        if data['priority_orig'].isdigit() == False:
            data['priority_orig'] = 'None'
#Delete double quotes and trips from some columns
        data['message_orig'] = re.sub(r'["\'-?:!;]', '', data['message_revised']).strip()
        data['message_revised'] = re.sub(r'["\'-?:!;]', '', data['message_revised']).strip()
#Print new message
        print(f"For sensor '{data['ï»¿intervalidentifier']}' original message from system is '{data['message_orig']}'. \n"
             f"In Grafana dashboard this message looks like '{data['message_revised']}'. \n"
             f"Priority for this alert is '{data['priority_orig']}' \n")
    file.close()


