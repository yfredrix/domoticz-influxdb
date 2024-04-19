import sqlite3
from dataclasses import dataclass
from typing import List


@dataclass
class EnergyStats:
    DeviceRowID: int
    Value1: int
    Value2: int
    Value3: int
    Value4: int
    Value5: int
    Value6: int
    Counter1: int
    Counter2: int
    Counter3: int
    Counter4: int
    Date: str


@dataclass
class GasStats:
    DeviceRowID: int
    Value: int
    Counter: int
    Date: str


class SqliteConnector:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def execute(self, query):
        self.cursor.execute(query)
        self.conn.commit()

    def fetchall(self):
        return self.cursor.fetchall()

    def close(self):
        self.conn.close()


class DomoticzEnergyClient(SqliteConnector):
    def get_energy_usage(self, hardware_name: str) -> List[EnergyStats]:
        self.connect()
        self.execute(f"SELECT ID FROM Hardware WHERE Name = '{hardware_name}'")
        results = self.fetchall()
        if not results or len(results) > 1:
            return [EnergyStats()]
        hardware_id = results[0][0]
        self.execute(
            f"SELECT ID FROM DeviceStatus WHERE HardwareID = '{hardware_id}' and Name = 'Power'"
        )
        results = self.fetchall()
        if not results or len(results) > 1:
            return [EnergyStats()]
        device_id = results[0][0]

        self.execute(
            f"SELECT * FROM MultiMeter_Calendar WHERE DeviceRowID = {device_id};"
        )
        columns = self.cursor.description
        results = self.fetchall()
        return [
            EnergyStats(**dict(zip([column[0] for column in columns], result)))
            for result in results
        ]

    def get_gas_usage(self, hardware_name: str) -> List[GasStats]:
        self.connect()
        self.execute(f"SELECT ID FROM Hardware WHERE Name = '{hardware_name}'")
        results = self.fetchall()
        if not results or len(results) > 1:
            return [GasStats()]
        hardware_id = results[0][0]
        self.execute(
            f"SELECT ID FROM DeviceStatus WHERE HardwareID = '{hardware_id}' and Name = 'Gas'"
        )
        results = self.fetchall()
        if not results or len(results) > 1:
            return [GasStats()]
        device_id = results[0][0]

        self.execute(f"SELECT * FROM Meter_Calendar WHERE DeviceRowID = {device_id};")
        columns = self.cursor.description
        results = self.fetchall()
        return [
            GasStats(**dict(zip([column[0] for column in columns], result)))
            for result in results
        ]
