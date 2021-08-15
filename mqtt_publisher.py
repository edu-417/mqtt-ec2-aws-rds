from paho.mqtt import client as mqtt
import threading, json, random
from datetime import datetime

MQTT_BROKER = 'ec2-18-224-61-52.us-east-2.compute.amazonaws.com'
MQTT_PORT = 1883
KEEP_ALIVE_INTERVAL = 60
MQTT_TOPIC_SENSOR_DATA = 'SensorData'
MQTT_USER = 'mqtt-user'
MQTT_PASSWORD = 'mqtt'

SENSOR_ANONYMOUS_VARIABLES_NUMBER = 8
SENSOR_ID_FIELD = 'sensor_id'
SENSOR_LATITUDE_FIELD = 'gps_latitude'
SENSOR_LONGITUDE_FIELD = 'gps_longitude'
SENSOR_ANONYMOUS_VARIABLE_FIELD_NAME = 'var'
SENSOR_ANONYMOUS_VARIABLE_FIELDS = [ SENSOR_ANONYMOUS_VARIABLE_FIELD_NAME + str(i) for i in range(SENSOR_ANONYMOUS_VARIABLES_NUMBER)]

def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("connected OK Returned code=", rc)
        client.subscribe(MQTT_TOPIC_SENSOR_DATA,)
    else:
        print("Bad connection Returned code=", rc)

def on_disconnect(client, userdata, rc):
    print("DisConnected result code ", str(rc))

def publish_fake_sensor_data(args):

    threading.Timer(2, publish_fake_sensor_data, [args]).start()


    random_sensor_id = random.randint(0, 8)
    random_sensor_latitude = random.gauss(-12, 0.05)
    random_sensor_longitude = random.gauss(-77, 0.05)

    sensor_data = {}
    sensor_data[SENSOR_ID_FIELD] = f'dummy-{random_sensor_id}'
    sensor_data[SENSOR_LATITUDE_FIELD] = f'{random_sensor_latitude}'
    sensor_data[SENSOR_LONGITUDE_FIELD] = f'{random_sensor_longitude}'
    
    client = args[0]

    for i in range(SENSOR_ANONYMOUS_VARIABLES_NUMBER):
        random_sensor_anonymous_value = random.uniform(150, 2000)
        sensor_data[SENSOR_ANONYMOUS_VARIABLE_FIELDS[i]] = f'{random_sensor_anonymous_value}'

    client.publish(MQTT_TOPIC_SENSOR_DATA, json.dumps(sensor_data) )

    
if __name__ == '__main__':

    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect

    mqtt_client.username_pw_set(username=MQTT_USER,password=MQTT_PASSWORD)
    mqtt_client.connect(host=MQTT_BROKER, port=MQTT_PORT, keepalive=KEEP_ALIVE_INTERVAL)

    publish_fake_sensor_data([mqtt_client])

