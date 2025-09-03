import asyncio
import pyfred


async def worker(redis, n):
    for i in range(200):
        await redis.ping()


async def main():
    redis = pyfred.Client()
    await redis.connect()
    tasks = [
        asyncio.create_task(worker(redis, i))
        for i in range(200)
    ]
    await asyncio.wait(tasks)


asyncio.run(main())
        