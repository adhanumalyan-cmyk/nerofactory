from .mes_connector import MESConnector
from .erp_connector import ERPConnector
from .mqtt_listener import MQTTListener
import duckdb

def get_connectors():
    # Open ONE connection for all
    conn = duckdb.connect('factory.duckdb')
    
    # Pass the same connection to both connectors
    mes = MESConnector(connection=conn)
    erp = ERPConnector(connection=conn)
    mqtt = MQTTListener()
    
    return mes, erp, mqtt