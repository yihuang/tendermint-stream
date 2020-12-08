"""
CREATE TABLE "transfer" (
    "id" bigserial,
    "height" int NOT NULL,
    "txindex" int,
    "evtindex" int NOT NULL,
    "sender" varchar NOT NULL,
    "recipient" varchar NOT NULL,
    "amount" varchar NOT NULL,
    PRIMARY KEY ("id")
);

CREATE INDEX "transfers_sender_idx" ON "transfer"("sender");
CREATE INDEX "transfers_recipient_idx" ON "transfer"("recipient");
CREATE UNIQUE INDEX "transfers_txindex_height_idx" ON "transfer"("height","txindex","evtindex");
"""
import base64
import time

import aiohttp
import ujson
from decouple import config

TENDERMINT_RPC = config("TENDERMINT_RPC", "http://localhost:26657")
PG_CONNSTR = config("PG_CONNSTR", "postgresql://indexing:123456@127.0.0.1/indexing")
BATCH_SIZE = 200


hit = miss = 0


async def fetch_json(session, url):
    global hit, miss
    async with session.get(url) as rsp:
        if rsp.status == 200:
            if int(rsp.headers.get("AGE", 0)) > 0:
                hit += 1
            else:
                miss += 1
            txt = await rsp.text()
            rsp = ujson.loads(txt)
            assert "error" not in rsp, txt
            return rsp["result"]


def parse_attrs(attrs):
    return {
        base64.b64decode(item["key"]).decode(): (
            base64.b64decode(item["value"]).decode()
            if item["value"] is not None
            else ""
        )
        for item in attrs
    }


def process_transfer_event(evt):
    attrs = parse_attrs(evt["attributes"])
    return (attrs["sender"], attrs["recipient"], attrs["amount"])


async def main():
    global hit, miss
    pg = await asyncpg.connect(PG_CONNSTR)
    offset = (
        await pg.fetchval(
            "select last_handled_event_height from projections where id=$1", "Transfer"
        )
        or 0
    ) + 1
    catched_up = False
    async with aiohttp.ClientSession() as session:
        while True:
            offsets = range(offset, offset + (1 if catched_up else BATCH_SIZE))
            urls = [
                f"{TENDERMINT_RPC}/block_results?height={offset}" for offset in offsets
            ]
            rsps = await asyncio.gather(*(fetch_json(session, url) for url in urls))

            rows = []
            for offset, rsp in zip(offsets, rsps):
                if rsp is None:
                    print("catched up", offset)
                    catched_up = True
                    break
                for ev in rsp["begin_block_events"]:
                    if ev["type"] == "transfer":
                        rows.append((offset, None, 0) + process_transfer_event(ev))
                for i, tx in enumerate(rsp["txs_results"] or []):
                    for j, ev in enumerate(tx["events"]):
                        if ev["type"] == "transfer":
                            rows.append((offset, i, j) + process_transfer_event(ev))
            if rows:
                async with pg.transaction():
                    # insert batch rows
                    await pg.copy_records_to_table(
                        "transfer",
                        records=rows,
                        columns=(
                            "height",
                            "txindex",
                            "evtindex",
                            "sender",
                            "recipient",
                            "amount",
                        ),
                    )
                    # save last_handled_event_height
                    await pg.execute(
                        "insert into projections values ($1, $2) "
                        "on conflict(id) do update set "
                        "last_handled_event_height=excluded.last_handled_event_height",
                        "Transfer",
                        offset,
                    )
            print(offset, time.time(), hit / (hit + miss))
            hit = miss = 0
            offset += 1


if __name__ == "__main__":
    import asyncio
    import asyncpg

    asyncio.run(main())
