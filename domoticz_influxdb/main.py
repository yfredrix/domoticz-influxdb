from domoticz import DomoticzEnergyClient
from influxdb import InfluxDBClient
from convert import convert_energyStats, convert_gasStats
import argparse

parser = argparse.ArgumentParser(description="Convert Domoticz energy data to InfluxDB")
parser.add_argument("--equipment_id", type=int, help="The equipment ID to convert")
parser.add_argument(
    "-db", "--database", type=str, help="The path to the domoticz database"
)
parser.add_argument(
    "-c", "--config", type=str, help="The path to the InfluxDB config file"
)
parser.add_argument("--hardware_name", type=str, help="The name of the hardware")
args = parser.parse_args()

equipment_id = args.equipment_id

energyConnector = DomoticzEnergyClient(args.database)

energystats = energyConnector.get_energy_usage(hardware_name=args.hardware_name)
gasStats = energyConnector.get_gas_usage(hardware_name=args.hardware_name)
influxDBEnergy = convert_energyStats(energystats)
influxDBGas = convert_gasStats(gasStats)

influxClient = InfluxDBClient(args.config)

for energy in influxDBEnergy:
    influx_point = influxClient.convert_to_point(
        energy.time,
        "electricity",
        [
            ("electricity_delivered_tariff_1", energy.electricity_delivered_tariff_1),
            ("electricity_delivered_tariff_2", energy.electricity_delivered_tariff_2),
            ("electricity_used_tariff_1", energy.electricity_used_tariff_1),
            ("electricity_used_tariff_2", energy.electricity_used_tariff_2),
        ],
        [("unit", energy.unit), ("equipment_id", equipment_id)],
    )
    influxClient.write(influx_point, "energy")

for gas in influxDBGas:
    influx_point = influxClient.convert_to_point(
        gas.time,
        "gas",
        [("hourly", gas.gas_used)],
        [("unit", gas.unit), ("equipment_id", equipment_id)],
    )
    influxClient.write(influx_point, "latest_gas")
