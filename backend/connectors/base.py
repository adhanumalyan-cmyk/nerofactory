from abc import ABC, abstractmethod
import pandas as pd

class DataConnector(ABC):
    @abstractmethod
    def connect(self): pass
    @abstractmethod
    def fetch_production_data(self, start_date=None, end_date=None) -> pd.DataFrame: pass
    @abstractmethod
    def fetch_inventory_data(self) -> pd.DataFrame: pass
    @abstractmethod
    def fetch_employee_data(self) -> pd.DataFrame: pass
    @abstractmethod
    def fetch_financial_data(self) -> pd.DataFrame: pass