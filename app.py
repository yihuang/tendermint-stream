import asyncio

import aiohttp
import ujson
from decouple import config
from fastapi import FastAPI
from starlette.responses import StreamingResponse

app = FastAPI()
LATEST_HEIGHT = 0
TENDERMINT_RPC = config("TENDERMINT_RPC", "http://localhost:26657")


async def load_block(session, n):
    async with session.get(f"{TENDERMINT_RPC}/block?height={n}") as rsp:
        if rsp.status == 200:
            txt = await rsp.text()
            rsp = ujson.loads(txt)
            assert "error" not in rsp, txt
            # TODO only indent for development
            return ujson.dumps(rsp["result"], indent=2)


async def subscribe_new_blocks(offset):
    async with aiohttp.ClientSession() as session:
        while True:
            rsp = await load_block(session, offset)
            if rsp is None:
                await asyncio.sleep(0.5)
                continue
            yield rsp
            offset += 1


@app.get("/new_block")
async def new_blocks(offset: int = 1):
    return StreamingResponse(subscribe_new_blocks(offset))


if __name__ == "__main__":
    from uvicorn import run

    run(app)
