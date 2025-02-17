import json
import os
import psycopg2
from dotenv import load_dotenv
from confluent_kafka import Consumer

# Loading environment variables
load_dotenv()

KAFKA_BROKER=os.getenv("KAFKA_BROKER")
KAFKA_TOPIC=os.getenv("KAFKA_TOPIC")
DBHOST=os.getenv("DBHOST")
DBNAME=os.getenv("DBNAME")
DBUSER=os.getenv("DBUSER")
DBPASS=os.getenv("DBPASS")

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id' : 'weather_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([KAFKA_TOPIC])

conn = psycopg2.connect(
    host=DBHOST,
    dbname=DBNAME,
    user=DBUSER,
    password=DBPASS
)

cursor = conn.cursor()

def save_data(data):
    try:
        name= data.get('name', 'Unknown')
        main= data.get('weather', {})[0].get('main', 'Unknown')
        description= data.get('weather', [{}])[0].get('description', 'Unknown')
        temp= data.get('main', {}).get('temp', None)
        feels_like= data.get('main', {}).get('feels_like', None)
        temp_min= data.get('main', {}).get('temp_min', None)
        temp_max= data.get('main', {}).get('temp_max', None)
        pressure= data.get('main', {}).get('pressure', None)
        humidity= data.get('main', {}).get('humidity', None)
        sea_level= data.get('main', {}).get('sea_level', None)
        grnd_level= data.get('main', {}).get('grnd_level', None)
        visibility= data.get('visibility', None)
        wind_speed= data.get('wind', {}).get('speed', None)
        wind_deg= data.get('wind', {}).get('deg', None)
        wind_gust= data.get('wind', {}).get('gust', None)
        clouds= data.get('clouds', {}).get('all', None)
        timestamp= data.get('dt', None)
        country= data.get('sys', {}).get('country', None)
        sunrise= data.get('sys', {}).get('sunrise', None)
        sunset= data.get('sys', {}).get('sunset', None)

        print("data stored!")

        cursor.execute("""
            INSERT INTO weatherdata (name, main, description, temp, feels_like, temp_min, temp_max, clouds, pressure, humidity, visibility, sea_level, grnd_level, wind_speed, wind_deg, wind_gust, timestamp, country, sunrise, sunset)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (name, main, description, temp, feels_like, temp_min, temp_max, clouds, pressure, humidity, visibility, sea_level,
            grnd_level, wind_speed, wind_deg, wind_gust, timestamp, country, sunrise, sunset))

        conn.commit()
        print("Data was stored successfully!")

    except Exception as e:
        print(f"Error saving to database {e}")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    try:
        raw_data = msg.value()
        print(f"Raw message: {raw_data}")
        data = json.loads(msg.value().decode('utf-8'))
        save_data(data)
    except Exception as e:
        print(f"Error processing: {e}")