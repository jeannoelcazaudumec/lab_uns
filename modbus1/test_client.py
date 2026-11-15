import asyncio
import os
import sys

from pymodbus.client import AsyncModbusTcpClient

HOST = os.getenv("MODBUS_HOST", "localhost")
PORT = int(os.getenv("MODBUS_PORT", "502"))


async def main():
    print(f"Connecting to Modbus TCP server at {HOST}:{PORT}...")
    client = AsyncModbusTcpClient(HOST, port=PORT)
    connected = await client.connect()

    if not connected:
        print(f"FAILED to connect to Modbus TCP server at {HOST}:{PORT}")
        sys.exit(1)

    print("Connected! Reading sensor holding registers for 5 iterations...\n")

    for i in range(1, 6):
        res = await client.read_holding_registers(address=0, count=4, slave=1)
        if not res.isError():
            temp_scaled, hum_scaled, temp_int, hum_int = res.registers
            temp_c = temp_scaled / 100.0
            hum_pct = hum_scaled / 100.0
            print(
                f"[{i}/5] Read Success -> Temp: {temp_c:.2f}°C, "
                f"Humidity: {hum_pct:.2f}% | Raw Regs: {res.registers}"
            )
        else:
            print(f"[{i}/5] Error reading registers: {res}")

        await asyncio.sleep(2)

    client.close()
    print("\nModbus client test completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
