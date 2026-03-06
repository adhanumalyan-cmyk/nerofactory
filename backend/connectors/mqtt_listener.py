import threading
import time
import random
import json
import pandas as pd
from .base import DataConnector

class MQTTListener(DataConnector):
    def __init__(self, broker="mqtt.factory.local", port=1883):
        self.broker = broker
        self.port = port
        self.messages = []
        self.running = False
        self.connected = False
    def connect(self):
        print("MQTT connected (simulated).")
        self.connected = True
        self.running = True
        self.thread = threading.Thread(target=self._simulate_messages)
        self.thread.daemon = True
        self.thread.start()
    def _simulate_messages(self):
        while self.running:
            msg = {
                'sensor_id': f"sensor_{random.randint(1,5)}",
                'value': random.uniform(20,30),
                'timestamp': time.time()
            }
            self.messages.append({'topic': 'sensors/temp', 'payload': json.dumps(msg)})
            time.sleep(random.randint(2,5))
    def fetch_production_data(self, start_date=None, end_date=None) -> pd.DataFrame:
        return pd.DataFrame(self.messages)
    def fetch_inventory_data(self) -> pd.DataFrame:
        return pd.DataFrame()
    def fetch_employee_data(self) -> pd.DataFrame:
        return pd.DataFrame()
    def fetch_financial_data(self) -> pd.DataFrame:
        return pd.DataFrame()