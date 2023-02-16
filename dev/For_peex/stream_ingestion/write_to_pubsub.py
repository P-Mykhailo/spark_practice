import os
import names
import datetime
import random
import time
from google.cloud import pubsub_v1

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'mykh_key.json'
publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/extreme-arch-376708/topics/mykh_pubsub'

def calculate_age(born):
    today = datetime.date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

def date_of_bith():
    start_date = datetime.date.today() - datetime.timedelta(weeks=5200)
    end_date = datetime.date.today() - datetime.timedelta(weeks=936)
    num_days = (end_date - start_date).days
    rand_days = random.randint(1, num_days)
    random_date = start_date + datetime.timedelta(days=rand_days)
    return random_date

for i in range(20):
    bithday = date_of_bith()
    data = f"""
    {{
    'sys_transaction_time': '{datetime.datetime.now()}'
    'first_name': '{names.get_first_name()}'
    'last_name': '{names.get_last_name()}'
    'bithday': '{bithday}'
    'age': '{calculate_age(bithday)}'
    'card_number': '{random.randint(100000000, 999999999)}'
    'total_sum': '{round(random.uniform(0.05, 2000.00),2)}'
    }}
    """
    data = data.encode('UTF-8')
    print(data)
    future = publisher.publish(topic_path, data)
    print(f'Published vessage id {future.result()}')
    time.sleep(2)