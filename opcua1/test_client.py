import asyncio
from asyncua import Client

async def main():
    url = "opc.tcp://opcua1:4840/freeopcua/server/"
    print(f"Connecting to {url}...")
    async with Client(url=url) as client:
        # Find namespace
        idx = await client.get_namespace_index("http://enterprise.plant/asset_3")
        print(f"Namespace index: {idx}")

        # Get variables
        temp_node = client.get_node(f"ns={idx};s=Temperature")
        hum_node = client.get_node(f"ns={idx};s=Humidity")
        speed_node = client.get_node(f"ns={idx};s=MotorSpeed")

        t_val = await temp_node.read_value()
        h_val = await hum_node.read_value()
        s_val = await speed_node.read_value()

        print(f"Read OK: Temperature={t_val}°C, Humidity={h_val}%, MotorSpeed={s_val} RPM")

if __name__ == "__main__":
    asyncio.run(main())
