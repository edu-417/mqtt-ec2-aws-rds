from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION

from sqlalchemy.orm import declarative_base, sessionmaker

from paho.mqtt import client as mqtt
import json, datetime


MQTT_BROKER = 'ec2-18-224-61-52.us-east-2.compute.amazonaws.com'
MQTT_PORT = 1883
KEEP_ALIVE_INTERVAL = 60
MQTT_TOPIC_SENSOR_DATA = 'SensorData'
MQTT_USER = 'mqtt-user'
MQTT_PASSWORD = 'mqtt'

AWS_RDS_HOST = 'mqtt-db.ctk0uhjswjzz.us-east-2.rds.amazonaws.com'
AWS_RDS_USER = 'postgres'
AWS_RDS_PASSWORD = 'I3Q9FBUUboCY4fK3cc0Z'
AWS_RDS_DATABASE = 'mqtt_database'

SENSOR_ANONYMOUS_VARIABLES_NUMBER = 8
SENSOR_ID_FIELD = 'sensor_id'
SENSOR_LATITUDE_FIELD = 'gps_latitude'
SENSOR_LONGITUDE_FIELD = 'gps_longitude'
SENSOR_ANONYMOUS_VARIABLE_FIELD_NAME = 'var'
SENSOR_ANONYMOUS_VARIABLE_FIELDS = [ SENSOR_ANONYMOUS_VARIABLE_FIELD_NAME + str(i) for i in range(SENSOR_ANONYMOUS_VARIABLES_NUMBER)]

Base = declarative_base()

class SensorData(Base):
    __tablename__ = 'sensor_data'

    id = Column('id', Integer, primary_key=True)
    sensor_id = Column(SENSOR_ID_FIELD, String(50), nullable=False)
    latitude = Column(SENSOR_LATITUDE_FIELD, DOUBLE_PRECISION)
    longitude = Column(SENSOR_LONGITUDE_FIELD, DOUBLE_PRECISION)
    anonymous_var_0 = Column(SENSOR_ANONYMOUS_VARIABLE_FIELDS[0], String(50))
    anonymous_var_1 = Column(SENSOR_ANONYMOUS_VARIABLE_FIELDS[1], String(50))
    anonymous_var_2 = Column(SENSOR_ANONYMOUS_VARIABLE_FIELDS[2], String(50))
    anonymous_var_3 = Column(SENSOR_ANONYMOUS_VARIABLE_FIELDS[3], String(50))
    anonymous_var_4 = Column(SENSOR_ANONYMOUS_VARIABLE_FIELDS[4], String(50))
    anonymous_var_5 = Column(SENSOR_ANONYMOUS_VARIABLE_FIELDS[5], String(50))
    anonymous_var_6 = Column(SENSOR_ANONYMOUS_VARIABLE_FIELDS[6], String(50))
    anonymous_var_7 = Column(SENSOR_ANONYMOUS_VARIABLE_FIELDS[7], String(50))
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

    def __repr__(self):
        return f"SensorData(id={self.id!r}, sensor_id={self.sensor_id!r}, latitude={self.latitude!r}, longitude={self.longitude!r})"

def init_db():
    engine = create_engine(f'postgresql+psycopg2://{AWS_RDS_USER}:{AWS_RDS_PASSWORD}@{AWS_RDS_HOST}/{AWS_RDS_DATABASE}')
    # Base.metadata.drop_all(engine)
    # Base.metadata.create_all(engine)
    Session = sessionmaker(engine)
    return engine, Session

def send_to_db(data, Session):
    data = data.decode('utf-8')
    json_dict = json.loads(data)

    sensor_id = json_dict[SENSOR_ID_FIELD]
    latitude = json_dict[SENSOR_LATITUDE_FIELD]
    longitude = json_dict[SENSOR_LONGITUDE_FIELD]

    anonymous_variables = [""] * SENSOR_ANONYMOUS_VARIABLES_NUMBER

    for i in range(SENSOR_ANONYMOUS_VARIABLES_NUMBER):
        anonymous_variables[i] = json_dict[SENSOR_ANONYMOUS_VARIABLE_FIELDS[i]]

    with Session.begin() as session:
        sensor = SensorData(sensor_id = sensor_id, latitude = latitude, longitude = longitude)
        sensor.anonymous_var_0 = anonymous_variables[0]
        sensor.anonymous_var_1 = anonymous_variables[1]
        sensor.anonymous_var_2 = anonymous_variables[2]
        sensor.anonymous_var_3 = anonymous_variables[3]
        sensor.anonymous_var_4 = anonymous_variables[4]
        sensor.anonymous_var_5 = anonymous_variables[5]
        sensor.anonymous_var_6 = anonymous_variables[6]
        sensor.anonymous_var_7 = anonymous_variables[7]
        session.add(sensor)
        session.commit()

def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("connected OK Returned code=", rc)
        client.subscribe(MQTT_TOPIC_SENSOR_DATA,)
    else:
        print("Bad connection Returned code=", rc)

def on_disconnect(client, userdata, rc):
    print("DisConnected result code ", str(rc))

if __name__ == '__main__':
    engine, Session = init_db()

    def on_message(client, userdata, message):
        topic = message.topic

        if topic == MQTT_TOPIC_SENSOR_DATA:
            data = message.payload
            send_to_db(data, Session)

    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_disconnect = on_disconnect

    mqtt_client.username_pw_set(username=MQTT_USER,password=MQTT_PASSWORD)
    mqtt_client.connect(host=MQTT_BROKER, port=MQTT_PORT, keepalive=KEEP_ALIVE_INTERVAL)
    mqtt_client.loop_forever()
