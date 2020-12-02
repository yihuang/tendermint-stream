import asyncio

import aiohttp
import ujson
from decouple import config
from fastapi import FastAPI
from starlette.responses import StreamingResponse

app = FastAPI()
LATEST_HEIGHT = 0
TENDERMINT_RPC = config("TENDERMINT_RPC", "http://localhost:26657")


async def load_one(session, n, endpoint):
    async with session.get(f"{TENDERMINT_RPC}{endpoint}?height={n}") as rsp:
        if rsp.status == 200:
            txt = await rsp.text()
            rsp = ujson.loads(txt)
            assert "error" not in rsp, txt
            # TODO only indent for development
            return ujson.dumps(rsp["result"], indent=2)


async def subscribe_rpc(offset, endpoint):
    async with aiohttp.ClientSession() as session:
        while True:
            rsp = await load_one(session, offset, endpoint)
            if rsp is None:
                await asyncio.sleep(0.5)
                continue
            yield rsp
            offset += 1


@app.get("/blocks")
async def blocks(offset: int = 1):
    return StreamingResponse(subscribe_rpc(offset, "/block"))


@app.get("/block_results")
async def block_results(offset: int = 1):
    return StreamingResponse(subscribe_rpc(offset, "/block_results"))


if __name__ == "__main__":
    from uvicorn import run

    run(app)
