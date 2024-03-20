import asyncio

from fastapi import FastAPI

from create_server import add_new_account, main, delete_account

app = FastAPI()


@app.get("/")
def read_root():
    return {"Manager": "is active"}


@app.get("/ws/manager/start")
async def start_ws_manager():
    print('Получена команда запуска')
    asyncio.create_task(main())
    print('Запуск ws-manager...')
    return {"message": "Запуск команды начат"}


@app.get("/ws/conn/new_account/{acc_pk}")
async def conn_to_new_account(acc_pk: int):  # Action create conn account
    await add_new_account(acc_pk)


@app.get("/ws/conn/del_account/{acc_pk}")
async def conn_to_new_account(acc_pk: int):  # Action delete account
    await delete_account(acc_pk)


@app.get("/ws/conn/update_account/{acc_pk}")
async def conn_to_new_account(acc_pk: int):  # Action update account
    await delete_account(acc_pk)
    await asyncio.sleep(5)
    await add_new_account(acc_pk)


if __name__ == "__main__":
    import uvicorn

    print("Uvicorn awake...")
    uvicorn.run(app, host="0.0.0.0", port=8008)
