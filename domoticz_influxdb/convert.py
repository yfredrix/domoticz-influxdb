import datetime
from domoticz import EnergyStats, GasStats
from dataclasses import dataclass
from typing import List


@dataclass
class InfluxDBEnergy:
    time: datetime.datetime
    unit: str
    electricity_delivered_tariff_1: float
    electricity_delivered_tariff_2: float
    electricity_used_tariff_1: float
    electricity_used_tariff_2: float


@dataclass
class InfluxDBGas:
    time: datetime.datetime
    unit: str
    gas_used: float


def convert_counter_to_kwh(counter: int) -> float:
    return counter / 1000


def convert_counter_to_m3(counter: int) -> float:
    return counter / 1000


def convert_date_to_datetime(date_string: str) -> datetime.datetime:
    date = datetime.date.fromisoformat(date_string)
    dt = datetime.datetime.combine(date, datetime.datetime.max.time())
    return dt


def convert_energyStats(energyStats: List[EnergyStats]) -> List[InfluxDBEnergy]:
    return [
        InfluxDBEnergy(
            time=convert_date_to_datetime(energyStat.Date),
            unit="kWh",
            electricity_delivered_tariff_1=convert_counter_to_kwh(energyStat.Counter2),
            electricity_delivered_tariff_2=convert_counter_to_kwh(energyStat.Counter4),
            electricity_used_tariff_1=convert_counter_to_kwh(energyStat.Counter1),
            electricity_used_tariff_2=convert_counter_to_kwh(energyStat.Counter3),
        )
        for energyStat in energyStats
    ]


def convert_gasStats(gasStats: List[GasStats]) -> List[InfluxDBGas]:
    return [
        InfluxDBGas(
            time=convert_date_to_datetime(gasStat.Date),
            unit="m3",
            gas_used=convert_counter_to_m3(gasStat.Counter),
        )
        for gasStat in gasStats
    ]
