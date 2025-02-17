import json
import requests
import os
from dotenv import load_dotenv
from confluent_kafka import Producer

# Loading .env variables
load_dotenv()

# Loading access keys
API_KEY=os.getenv("API")
CITY=os.getenv("CITY")
KAFKA_BROKER=os.getenv("KAFKA_BROKER")
KAFKA_TOPIC=os.getenv("KAFKA_TOPIC")

conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(conf)

def fetch_weather():
    url= f"https://api.openweathermap.org/data/2.5/weather?id={CITY}&appid={API_KEY}&units=imperial"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def send_to_kafka(data):
    try:
        producer.produce(KAFKA_TOPIC, json.dumps(data))
        producer.flush()
        print(f"Weather Data has been sent to be processed")
    except Exception as e:
        print(f"Theres was an error sending data {e}")

if __name__ == "__main__":
    weather_data = fetch_weather()
    if weather_data:
        send_to_kafka(weather_data)