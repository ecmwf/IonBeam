import asyncio

from ionbeam.apps.faststream import factory


async def main():
    app = await factory()
    await app.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        pass
