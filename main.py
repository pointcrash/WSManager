import asyncio
import logging
from fastapi.responses import JSONResponse
from fastapi import FastAPI, HTTPException

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


# @app.get("/ws/conn/new_account/{acc_pk}")
# async def conn_account_status_check_view(acc_pk: int):  # Action create conn account
#     await add_new_account(acc_pk)


@app.get("/ws/conn/new_account/{acc_pk}")
async def conn_to_new_account_view(acc_pk: int):  # Action create conn account
    await add_new_account(acc_pk)


@app.get("/ws/conn/del_account/{acc_pk}")
async def delete_account_view(acc_pk: int):  # Action delete account
    await delete_account(acc_pk)


@app.get("/ws/conn/update_account/{acc_pk}")
async def reconnect_account_view(acc_pk: int):  # Action update account
    try:
        logging.info('account update')
        await delete_account(acc_pk)
        logging.info('account deleted')
        await asyncio.sleep(5)
        await add_new_account(acc_pk)
        logging.info('account updated')

        return JSONResponse(
            status_code=200,
            content={"success": "True", "message": f"Account {acc_pk} updated successfully"}
        )

    except Exception as e:
        logging.error(f"Error updating account {acc_pk}: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": "False",
                "message": f"Account {acc_pk} updated unsuccessfully",
                "error": str(e)
            }
        )


if __name__ == "__main__":
    import uvicorn

    print("Uvicorn awake...")
    uvicorn.run(app, host="0.0.0.0", port=8008)
