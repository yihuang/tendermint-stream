"""
CREATE TABLE "transfer" (
    "id" bigserial,
    "height" int NOT NULL,
    "txindex" int,
    "sender" varchar NOT NULL,
    "recipient" varchar NOT NULL,
    "amount" varchar NOT NULL,
    PRIMARY KEY ("id")
);

CREATE INDEX "transfers_sender_idx" ON "transfer"("sender");
CREATE INDEX "transfers_recipient_idx" ON "transfer"("recipient");
CREATE UNIQUE INDEX "transfers_txindex_height_idx" ON "transfer"("txindex","height");
"""
import base64

import aiohttp
import ujson
from decouple import config

TENDERMINT_RPC = config("TENDERMINT_RPC", "http://localhost:26657")
PG_CONNSTR = config("PG_CONNSTR", "postgresql://indexing:123456@127.0.0.1/indexing")


async def load_block_results(session, n):
    async with session.get(f"{TENDERMINT_RPC}/block_results?height={n}") as rsp:
        if rsp.status == 200:
            txt = await rsp.text()
            rsp = ujson.loads(txt)
            assert "error" not in rsp, txt
            return rsp["result"]


def parse_attrs(attrs):
    return {
        base64.b64decode(item["key"]).decode(): base64.b64decode(item["value"]).decode()
        for item in attrs
    }


def process_transfer_event(evt):
    attrs = parse_attrs(evt["attributes"])
    return (attrs["sender"], attrs["recipient"], attrs["amount"])


async def main():
    pg = await asyncpg.connect(PG_CONNSTR)
    offset = (
        await pg.fetchval(
            "select last_handled_event_height from projections where id=$1", "Transfer"
        )
        or 1
    )
    async with aiohttp.ClientSession() as session:
        while True:
            rows = []
            rsp = await load_block_results(session, offset)
            if rsp is None:
                await asyncio.sleep(0.5)
                continue
            for ev in rsp["begin_block_events"]:
                if ev["type"] == "transfer":
                    rows.append((offset, None) + process_transfer_event(ev))
            for i, tx in enumerate(rsp["txs_results"] or []):
                for ev in tx["events"]:
                    if ev["type"] == "transfer":
                        rows.append((offset, i) + process_transfer_event(ev))
            if rows:
                async with pg.transaction():
                    # insert batch rows
                    await pg.copy_records_to_table(
                        "transfer",
                        records=rows,
                        columns=("height", "txindex", "sender", "recipient", "amount"),
                    )
                    # save last_handled_event_height
                    await pg.execute(
                        "insert into projections values ($1, $2) "
                        "on conflict(id) do update set "
                        "last_handled_event_height=excluded.last_handled_event_height",
                        "Transfer",
                        offset,
                    )
            offset += 1


if __name__ == "__main__":
    import asyncio
    import asyncpg

    asyncio.run(main())
