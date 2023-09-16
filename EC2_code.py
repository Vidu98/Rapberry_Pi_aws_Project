import paho.mqtt.client as mqtt
import psycopg2
import json

# MQTT settings
mqtt_broker_host = "test.mosquitto.org"  # Assuming the MQTT broker is running on the same EC2 instance
mqtt_topic  = "temperature_humidity"

# PostgreSQL settings
db_host = "localhost"
db_name = "yourdb"
db_user = "youruser"
db_password = "Rmvc@1998"

def on_message(client, userdata, message):
    payload = message.payload.decode()
    data = json.loads(payload)
    temperature = data["temperature"]
    humidity = data["humidity"]

    connection = psycopg2.connect(
        host=db_host, database=db_name, user=db_user, password=db_password
    )
    cursor = connection.cursor()
    insert_query = "INSERT INTO sensor_data (temperature, humidity) VALUES (%s, %s);"
    cursor.execute(insert_query, (temperature, humidity))
    connection.commit()
    cursor.close()
    connection.close()

mqtt_client = mqtt.Client()
mqtt_client.on_message = on_message
mqtt_client.connect(mqtt_broker_host)
mqtt_client.subscribe(mqtt_topic)

mqtt_client.loop_forever()
