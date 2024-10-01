import json
from faker import Faker
from faker_clickstream import ClickstreamProvider
import time
import os

FOLDER = '../data/clickstream_data'
TIME_TO_SLEEP = 5
MAX_EVENTS = 20


def generate_clickstream():

    fake = Faker()
    fake.add_provider(ClickstreamProvider)
    data = []

    while True:
        event = fake.session_clickstream(rand_session_max_size=MAX_EVENTS)
        data.append(event)
        os.makedirs(FOLDER, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(FOLDER, f"clickstream_{timestamp}.json")
        with open(filename, 'w') as f:
            json.dump(data[0], f)
            print(f"Saved as: {filename}")
        data = []
        time.sleep(TIME_TO_SLEEP)


if __name__ == "__main__":
    generate_clickstream()
