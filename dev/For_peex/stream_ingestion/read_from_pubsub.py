import os
from google.cloud import pubsub_v1

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'mykh_key.json'

subsciber = pubsub_v1.subscriber.Client()

subscription_path = 'projects/extreme-arch-376708/subscriptions/mykh_pubsub-sub'

def callback(message):
    #print(f'recived message {message}')
    print(f'data: {message.data}')
    message.ack()

streaming_pull = subsciber.subscribe(subscription_path, callback=callback)
print(f'listing of messages of {subscription_path}')

with subsciber:
    try:
        streaming_pull.result()
        streaming_pull.awaitTermination(120)
        streaming_pull.stop()

    except TimeoutError:
        streaming_pull.cancel()
        streaming_pull.result()