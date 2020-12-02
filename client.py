import asyncio
import json

import aiohttp


async def amain(offset, url):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{url}?offset={offset}") as rsp:
            while True:
                msg = await rsp.content.readany()
                if msg is None:
                    break
                print("block result", json.loads(msg)["height"])


def main(offset=1, url="http://localhost:8000/block_results"):
    asyncio.run(amain(offset, url))


if __name__ == "__main__":
    import fire

    fire.Fire(main)
