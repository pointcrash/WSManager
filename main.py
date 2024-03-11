import asyncio
from typing import Union

from fastapi import FastAPI

from create_server import add_new_account

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/ws/conn/new_account/{acc_pk}")
def conn_to_new_account(acc_pk: int):
    task = asyncio.create_task(add_new_account(acc_pk))
