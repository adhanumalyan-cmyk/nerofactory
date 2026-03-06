import pandas as pd
from .base import DataConnector

class ERPConnector(DataConnector):
    def __init__(self, connection):
        self.conn = connection
        self.connected = True

    def connect(self):
        pass

    def fetch_production_data(self, start_date=None, end_date=None) -> pd.DataFrame:
        return pd.DataFrame()
    def fetch_inventory_data(self) -> pd.DataFrame:
        return self.conn.execute("SELECT * FROM inventory").df()
    def fetch_employee_data(self) -> pd.DataFrame:
        return self.conn.execute("SELECT * FROM employees").df()
    def fetch_financial_data(self) -> pd.DataFrame:
        return self.conn.execute("SELECT * FROM transactions").df()