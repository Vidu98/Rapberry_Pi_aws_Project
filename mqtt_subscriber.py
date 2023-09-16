import paho.mqtt.client as mqtt
import psycopg2
from datetime import datetime

# MQTT configuration
MQTT_BROKER_HOST = "test.mosquitto.org"
MQTT_TOPIC = "sensor/temperature_humidity"

# PostgreSQL configuration
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "sensor_data"
DB_USER = "postgres"
DB_PASSWORD = "Rmvc@1998"

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    print("Received message:", payload)
    
    temperature = float(payload.split(',')[0].split('=')[1][:-2])
    humidity = float(payload.split(',')[1].split('=')[1][:-1])
    timestamp = datetime.now()

    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = connection.cursor()

        insert_query = "INSERT INTO sensor_data (temperature, humidity, timestamp) VALUES (%s, %s, %s)"
        cursor.execute(insert_query, (temperature, humidity, timestamp))
        connection.commit()

        print("Data inserted into PostgreSQL")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER_HOST, 1883, 60)

client.loop_forever()
