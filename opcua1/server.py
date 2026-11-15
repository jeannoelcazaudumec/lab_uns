import asyncio
import logging
import os
import random
import sys
from asyncua import Server, ua

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("opcua1_server")

OPCUA_HOST = os.getenv("OPCUA_HOST", "0.0.0.0")
OPCUA_PORT = int(os.getenv("OPCUA_PORT", "4840"))
ENDPOINT_URL = f"opc.tcp://{OPCUA_HOST}:{OPCUA_PORT}/freeopcua/server/"
SIM_INTERVAL = float(os.getenv("SIM_INTERVAL", "2.0"))


async def main():
    server = Server()
    await server.init()
    server.set_endpoint(ENDPOINT_URL)
    server.set_server_name("Asset 3 OPC UA Simulator")

    # Set up security policies to allow anonymous / NoSecurity
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

    # Register custom namespace
    uri = "http://enterprise.plant/asset_3"
    idx = await server.register_namespace(uri)

    # Get Objects node
    objects = server.nodes.objects

    # Add Device Object
    device_obj = await objects.add_object(idx, "Asset_3")

    # Add variables: Temperature, Humidity, MotorSpeed
    temp_var = await device_obj.add_variable(
        ua.NodeId("Temperature", idx), "Temperature", 25.0, ua.VariantType.Double
    )
    hum_var = await device_obj.add_variable(
        ua.NodeId("Humidity", idx), "Humidity", 50.0, ua.VariantType.Double
    )
    speed_var = await device_obj.add_variable(
        ua.NodeId("MotorSpeed", idx), "MotorSpeed", 1500.0, ua.VariantType.Double
    )

    await temp_var.set_writable()
    await hum_var.set_writable()
    await speed_var.set_writable()

    logger.info(f"Starting OPC UA Server on {ENDPOINT_URL}...")
    logger.info(
        f"Variables exposed: ns={idx};s=Temperature, ns={idx};s=Humidity, ns={idx};s=MotorSpeed"
    )

    temp = 25.0
    humidity = 50.0
    speed = 1500.0

    async with server:
        while True:
            try:
                # Fluctuate Temperature: 20.0 - 35.0 °C
                temp += random.uniform(-0.6, 0.6)
                temp = round(max(20.0, min(35.0, temp)), 2)

                # Fluctuate Humidity: 40.0 - 65.0 %
                humidity += random.uniform(-1.0, 1.0)
                humidity = round(max(40.0, min(65.0, humidity)), 2)

                # Fluctuate MotorSpeed: 1200 - 1800 RPM
                speed += random.uniform(-30.0, 30.0)
                speed = round(max(1200.0, min(1800.0, speed)), 1)

                await temp_var.write_value(temp)
                await hum_var.write_value(humidity)
                await speed_var.write_value(speed)

                logger.info(
                    f"[OPC-UA SIM] Temp: {temp:.2f}°C, Humidity: {humidity:.2f}%, MotorSpeed: {speed:.1f} RPM"
                )
            except Exception as e:
                logger.error(f"Error updating values: {e}")

            await asyncio.sleep(SIM_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("OPC UA Server stopped.")
