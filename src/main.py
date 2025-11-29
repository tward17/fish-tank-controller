import network
import time
import machine
import asyncio
import secrets
import config
import sys
import onewire, ds18x20, dht
from umqtt.robust import MQTTClient

# WiFi connection details
SSID = secrets.SSID
PASSWORD = secrets.WIFI_PASSWORD

# MQTT Broker details
MQTT_BROKER = config.MQTT_BROKER_ADDRESS 
MQTT_PORT = config.MQTT_BROKER_PORT
MQTT_CLIENT_ID = 'fish_tank_controller'

# HEARTBEAT
HEARTBEAT_MINUTES = config.MONITOR_HEARTBEAT_MINUTES

# FEATURES ENABLED
MONITOR_WATER_TEMPERATURE = config.MONITOR_WATER_TEMPERATURE_ENABLED
MONITOR_WATER_TEMPERATURE_POLLING_SECONDS = config.MONITOR_WATER_TEMPERATURE_POLLING_SECONDS
MONITOR_DHT22 = config.MONITOR_DHT22_ENABLED
MONITOR_DHT22_POLLING_SECONDS = config.MONITOR_DHT22_POLLING_SECONDS

# MQTT Sub Topics

#MQTT Pub Topics
FISHTANK_WATER_TEMPERATURE = 'fish_tank/water_temperature'
FISHTANK_AIR_TEMPERATURE = 'fish_tank/air_temperature'
FISHTANK_AIR_HUMIDITY = 'fish_tank/air_humidity'

#GPIO Pin Assignments
DS_SENSOR_PIN = 27
DHT22_SENSOR_PIN = 28

class DS18B20Monitor:
    def  __init__(self, mqtt_client) -> None:
        self.client = mqtt_client
        self.sensor = ds18x20.DS18X20(onewire.OneWire(machine.Pin(DS_SENSOR_PIN)))
        self.roms = self.sensor.scan()
        self.temperature = int(0)
        self.pollsSinceLastPublishCount = 0
        self.heartBeatLimit = (HEARTBEAT_MINUTES * 60) / MONITOR_WATER_TEMPERATURE_POLLING_SECONDS

    async def monitor_status(self):
        while True:
            try:
                print('Checking DS18B20')
                self.sensor.convert_temp()
                time.sleep_ms(750)

                for rom in self.roms:
                    current_temperature = round(self.sensor.read_temp(rom) + 0.7, 1)

                    self.pollsSinceLastPublishCount+=1

                    if self.temperature != current_temperature or self.pollsSinceLastPublishCount >= self.heartBeatLimit:
                        self.update_temperature(current_temperature)
                        
                    del current_temperature

                await asyncio.sleep(MONITOR_WATER_TEMPERATURE_POLLING_SECONDS)

            except OSError as e:
                print('Failed to read DS18B20 Sensor')

            

    def update_temperature(self, temperature):
        self.temperature = temperature
        print('DS18B20 Temperature change to: ' + str(self.temperature))
        publish_message(self.client,FISHTANK_WATER_TEMPERATURE, str(self.temperature))
        self.pollsSinceLastPublishCount = 0

class DHT22Monitor:
    def __init__(self, mqtt_client):
        self.client = mqtt_client
        self.sensor = dht.DHT22(machine.Pin(DHT22_SENSOR_PIN))
        self.temperature = int(0)
        self.humidity = int(0)
        self.pollsSinceLastTemperaturePublishCount = 0
        self.pollsSinceLastHumidityPublishCount = 0
        self.heartBeatLimit = (HEARTBEAT_MINUTES * 60) / MONITOR_DHT22_POLLING_SECONDS

    async def monitor_status(self):
        while True:
            try:
                print('Checking DHT22')
                self.sensor.measure()
                current_temperature = round(self.sensor.temperature(),1)
                current_humidity = round(self.sensor.humidity(),1)

                if self.temperature != current_temperature or self.pollsSinceLastTemperaturePublishCount >= self.heartBeatLimit:
                    self.update_temperature(current_temperature)

                if self.humidity != current_humidity or self.pollsSinceLastHumidityPublishCount >= self.heartBeatLimit:
                    self.update_humidity(current_humidity)

                del current_temperature, current_humidity

                await asyncio.sleep(MONITOR_DHT22_POLLING_SECONDS)

            except OSError as e:
                print('Failed to read DHT22 sensor.')

    def update_temperature(self, temperature):
        self.temperature = temperature
        print('DHT22 Temperature change to: ' + str(self.temperature))
        publish_message(self.client,FISHTANK_AIR_TEMPERATURE, str(self.temperature))
        self.pollsSinceLastTemperaturePublishCount = 0

    def update_humidity(self, humidity):
        self.humidity = humidity
        print('DHT22 Humidity change to: ' + str(self.humidity))
        publish_message(self.client,FISHTANK_AIR_HUMIDITY, str(self.humidity))
        self.pollsSinceLastHumidityPublishCount = 0

global wlan

# Connect to Wi-Fi
def connect_wifi():
    global wlan
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    
    if not wlan.isconnected():
        print('Connecting to WiFi...')
        wlan.connect(SSID, PASSWORD)
        i = 0
        while not wlan.isconnected():
            time.sleep(1)
            i += 1
            
            if i > 30:
                print('Could not connect to Wifi.')
                sys.exit()

        print('WiFi connected')
        print('IP address:', wlan.ifconfig()[0])
    else:
        print('Connected to Wifi')

    return wlan

# Callback function for handling incoming messages
def mqtt_callback(topic, msg):
    print('Received message on topic:', topic)
    print('Message:', msg)
    
# Connect to MQTT Broker
def connect_mqtt():
    client = MQTTClient(MQTT_CLIENT_ID, MQTT_BROKER, port=MQTT_PORT)
    client.set_callback(mqtt_callback)
    client.connect()
    print('Connected to MQTT broker:', MQTT_BROKER)
    return client

# Publish a message to MQTT topic
def publish_message(client, topic, message):
    print('Publishing message to topic:', topic)
    client.publish(topic, message)

# Main logic to run the MQTT client
async def main():

    # Connect to Wi-Fi
    global wlan
    if secrets.SSID != '' and secrets.WIFI_PASSWORD != '':
        connect_wifi()
    else:
        print('No Wifi details in secrets file. Exiting.')
        sys.exit()

    # Connect to MQTT broker
    client = connect_mqtt()

    # Subscribe to necessary topics

    if MONITOR_WATER_TEMPERATURE:
        ds18b20 = DS18B20Monitor(client)
        asyncio.create_task(ds18b20.monitor_status())

    if MONITOR_DHT22:
        dht22 = DHT22Monitor(client)
        asyncio.create_task(dht22.monitor_status())

    try:
        while True:
            if not wlan.isconnected():
                wlan = connect_wifi()

            # Check for any incoming MQTT messages
            client.check_msg()
            await asyncio.sleep(0.1) # Small delay to prevent overloading the loop
            #time.sleep(0.1)  

    except KeyboardInterrupt:
        print('Disconnecting...')
        client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
