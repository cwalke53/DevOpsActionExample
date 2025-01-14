import requests
from confluent_kafka import Producer
import socket
import json
import time
import random

KAFKA_TOPIC = 'pokemon'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:56864'
REQUEST_URL = "https://pokeapi.co/api/v2/pokemon?limit=1032"

def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s' % err)
    else:
        print('%% Message delivered to %s [%d]' % (msg.topic(), msg.partition()))



def process_pokemon(pokemon):
    if not ('url' in pokemon):
        # already processed
        return pokemon

    # Request details
    details_res = requests.get(pokemon['url'])
    details = details_res.json()
    pokemon['id'] = details['id']
    pokemon['weight'] = details['weight']
    pokemon['height'] = details['height']
    pokemon['moves'] = len(details['moves'])
    pokemon['stats'] = {
        "attack": get_stat(details, "attack"),
        "defense": get_stat(details, "defense"),
    }

    # Request species info
    species_res = requests.get(details['species']['url'])
    species_details = species_res.json()
    pokemon['color'] = species_details['color']['name']
    pokemon['is_mythical'] = species_details['is_mythical']

    del pokemon['url']

    return pokemon


def get_stat(details, name):
    stat_obj = next(filter(lambda stat: stat['stat']['name'] == name, details['stats']))
    if (not stat_obj):
        return 0
    else:
        return stat_obj['base_stat']


def main():
    # Kafka producer configuration
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': socket.gethostname()}

    # Create Kafka producer instance
    producer = Producer(conf)

    # Read the Wikimedia stream and produce messages to Kafka
    response = requests.get(REQUEST_URL)
    json_res = response.json()
    pokemon_list = json_res['results']
    while True:
        random.shuffle(pokemon_list)
        for pokemon in pokemon_list:
            pokemon_data = process_pokemon(pokemon)
            data = json.dumps(pokemon_data)
            try:
                producer.produce(KAFKA_TOPIC, data, callback=delivery_callback)
                # Flush messages to Kafka to ensure they are sent immediately
                producer.flush()
            except ValueError:
                pass
            time.sleep(1)

    # Close Kafka producer
    producer.close()

if __name__ == '__main__':
    main()
