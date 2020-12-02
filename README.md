Proof-of-concept implementation of reliable(at least once delivery) event streaming for tendermint rpc.

- Use long-polling
- Use block height as subscription offset
- Don't verify block data, use with trusted full-node

## TODO

- filter events

## Setup

```shell
$ poetry shell
```

Or

```
$ nix-shell
```

## Run

```shell
$ TENDERMINT_RPC=http://localhost:26657 uvicorn app:app
```

```shell
$ curl "http://localhost:8000/new_block?offset=10"
```

`client.py` subscribe messges programatically.
