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
                print('block', json.loads(msg)["block"]["header"]["height"])


def main(offset=1, url="http://localhost:8000/new_block"):
    asyncio.run(amain(offset, url))


if __name__ == "__main__":
    import fire
    fire.Fire(main)
