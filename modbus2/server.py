import asyncio
import logging
import os
import random
import sys

from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSlaveContext,
)
from pymodbus.server import StartAsyncTcpServer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("modbus2_server")

# Configuration from environment variables
SIM_INTERVAL = float(os.getenv("SIM_INTERVAL", "10.0"))
MODBUS_PORT = int(os.getenv("MODBUS_PORT", "502"))
MODBUS_HOST = os.getenv("MODBUS_HOST", "0.0.0.0")

# Initial sensor state
temp = 25.0
humidity = 50.0


async def sensor_simulation_loop(context: ModbusServerContext):
    """Background task to simulate sensor data and update Modbus registers."""
    global temp, humidity
    logger.info(f"Sensor simulation task active. Update interval: {SIM_INTERVAL}s")

    while True:
        try:
            # Fluctuate temperature between 20°C and 30°C
            temp_delta = random.uniform(-0.8, 0.8)
            temp = max(20.0, min(30.0, temp + temp_delta))

            # Fluctuate humidity between 40% and 60%
            hum_delta = random.uniform(-1.5, 1.5)
            humidity = max(40.0, min(60.0, humidity + hum_delta))

            # Register representations:
            # - Reg 0: Temperature * 100 (e.g., 25.34°C -> 2534)
            # - Reg 1: Humidity * 100 (e.g., 52.10% -> 5210)
            # - Reg 2: Temperature (integer part, e.g., 25)
            # - Reg 3: Humidity (integer part, e.g., 52)
            temp_scaled = int(round(temp * 100))
            hum_scaled = int(round(humidity * 100))
            temp_int = int(round(temp))
            hum_int = int(round(humidity))

            values = [temp_scaled, hum_scaled, temp_int, hum_int]

            # Update Holding Registers (FC 3) & Input Registers (FC 4)
            slave_id = 0
            context[slave_id].setValues(3, 0, values)
            context[slave_id].setValues(4, 0, values)

            logger.info(
                f"[SENSOR SIM] Temp: {temp:.2f}°C (Reg0: {temp_scaled}), "
                f"Humidity: {humidity:.2f}% (Reg1: {hum_scaled}) | updated every {SIM_INTERVAL}s"
            )
        except Exception as e:
            logger.error(f"Error in sensor simulation loop: {e}")

        await asyncio.sleep(SIM_INTERVAL)


async def main():
    # Initialize Modbus slave context with 100 registers initialized to 0
    store = ModbusSlaveContext(
        hr=ModbusSequentialDataBlock(0, [0] * 100),
        ir=ModbusSequentialDataBlock(0, [0] * 100),
        zero_mode=True,
    )
    context = ModbusServerContext(slaves=store, single=True)

    logger.info(f"Starting Modbus TCP Server on {MODBUS_HOST}:{MODBUS_PORT}...")

    # Start background sensor simulation task
    sim_task = asyncio.create_task(sensor_simulation_loop(context))

    try:
        await StartAsyncTcpServer(
            context=context,
            address=(MODBUS_HOST, MODBUS_PORT),
        )
    except asyncio.CancelledError:
        logger.info("Modbus server shutting down...")
    finally:
        sim_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Modbus server stopped.")
