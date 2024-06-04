import asyncio
import websockets
import logging

from ciclic_queue import CyclicQueue
from conn_to_binance import *
from conn_to_bybit import *
from db import save_to_db, update_wsmanager_status, create_conn_account_to_db, add_to_ws_ids_dict
from format import get_accounts, get_account, format_kline_interval_to_binance
from global_variables import active_connections, queue_dict, account_names, TASK_DICT, ws_ids

ws_locker = threading.Lock()


async def sub_to_kline(acc_name, service, symbol, interval):
    if service == 'ByBit':
        _queue = queue_dict[acc_name]
        bybit_sub_to_kline(acc_name, interval, symbol, _queue)

    elif service == 'Binance':
        interval = format_kline_interval_to_binance(interval)
        task = asyncio.create_task(
            sub_to_binance_kline_topik(acc_name, symbol, interval))
        return task


async def sub_to_mark_price(acc_name, service, symbol):
    task = None
    if service == 'ByBit':
        _queue = CyclicQueue(maxsize=1)
        task = asyncio.create_task(bybit_mark_price_sender(acc_name, _queue))
        bybit_sub_to_mp(acc_name, symbol, _queue)

    elif service == 'Binance':
        task = asyncio.create_task(sub_to_binance_market_price_topik(acc_name, symbol))
    return task


async def conn_handler(websocket, path):
    print("Client connected")
    logging.info("Client connected")
    acc_name = None
    service = None
    tasks = list()
    bybit_subs = []

    try:
        async for message in websocket:
            message = json.loads(message)
            print(message)
            if message['title'] == 'conn':
                acc_name = message['account']

                if acc_name in account_names:
                    if not active_connections.get(acc_name):
                        active_connections[acc_name] = set()
                    active_connections[acc_name].add(websocket)
                else:
                    acc_name = None
                    await websocket.send('Указано неверное имя акканута')

            elif message['title'] == 'sub':
                topic = message['topic']
                symbol = message['symbol']
                service = message['service']

                if acc_name:
                    if topic == 'mark_price':
                        if service == 'ByBit':
                            bybit_subs.append(('tickers', symbol))
                        task = await sub_to_mark_price(acc_name, service, symbol)
                        tasks.append(task)

                    if topic == 'kline':
                        interval = message['interval']
                        if service == 'ByBit':
                            bybit_subs.append(('kline', interval, symbol))
                        task = await sub_to_kline(acc_name, service, symbol, interval)
                        if task:
                            tasks.append(task)

                else:
                    await websocket.send('Нет подписки на аккаунт')

    except websockets.exceptions.ConnectionClosedError:
        print("Error - Client disconnected")
        logging.info("Client disconnected")
    except Exception as e:
        print(e)
    finally:
        if acc_name:
            active_connections[acc_name].remove(websocket)
        for task in tasks:
            task.cancel()
        for sub in bybit_subs:
            unsub_from_topic(acc_name, sub)
        print("Connection closed")


async def binance_sender(acc_id, acc_name, _queue):
    try:
        # print(f"Sender '{acc_name}' started")
        logging.info(f"Sender '{acc_name}' started")
        while True:
            data = await _queue.get()
            # logging.info(f"Data '{data}'")
            await save_to_db(acc_id, data)

            if active_connections.get(acc_name):
                for websocket in active_connections[acc_name]:
                    try:
                        await websocket.send(json.dumps(data))
                    except Exception as e:
                        logging.error(f"binance_sender error '{e}'")
                        print(e)
    finally:
        logging.info(f"Sender binance_sender '{acc_name}' stopped")


async def bybit_sender(acc_id, acc_name, _queue):
    try:
        # print(f"Sender '{acc_name}' started")
        logging.info(f"Sender '{acc_name}' started")
        while True:
            if _queue.qsize() > 0:
                data = _queue.get_nowait()
                # logging.info(f"Data '{data}'")
                await save_to_db(acc_id, data)

                if active_connections.get(acc_name):
                    for websocket in active_connections[acc_name]:
                        try:
                            await websocket.send(json.dumps(data))
                        except Exception as e:
                            logging.error(f"bybit_sender error '{e}'")
                            print(e)
            else:
                await asyncio.sleep(1)
    finally:
        logging.info(f"Sender bybit_sender '{acc_name}' stopped")


async def bybit_mark_price_sender(acc_name, _queue):
    try:
        # print(f"Sender '{acc_name}' started")
        logging.info(f"Sender '{acc_name}' started")
        while True:
            if _queue.qsize() > 0:
                data = _queue.get_nowait()
                # print(f"'{acc_name}' sending data:", data)

                if active_connections.get(acc_name):
                    for websocket in active_connections[acc_name]:
                        try:
                            await websocket.send(json.dumps(data))
                        except Exception as e:
                            logging.error(f"bybit_mark_price_sender error '{e}'")
                            print(e)
                await asyncio.sleep(2.9)
            else:
                await asyncio.sleep(3)
    finally:
        logging.info(f"Sender bb mark price '{acc_name}' stopped")


async def start_server():
    server = await websockets.serve(conn_handler, "0.0.0.0", 8765)
    await server.wait_closed()


async def add_new_account(acc_pk):
    account = await get_account(acc_pk)
    if account.name not in account_names:
        account_names.append(account.name)
    await new_connect(account)


async def delete_account(acc_pk):
    async def get_account_for_del():
        for account in TASK_DICT:
            if account.id == acc_pk:
                return account

    account = await get_account_for_del()
    await close_connection(account)


async def new_connect(account):
    with ws_locker:
        service_name = await account.service_name
        _queue = asyncio.Queue()
        queue_dict[account.name] = _queue
        TASK_DICT[account] = []

        if service_name == 'Binance':
            task_start_binance_sender = asyncio.create_task(binance_sender(account.id, account.name, _queue))
            await connect_to_binance_client(account)
            task_sub_binance_user = asyncio.create_task(sub_to_binance_user_topik(account.name))

            TASK_DICT[account].append(task_sub_binance_user)
            TASK_DICT[account].append(task_start_binance_sender)

        elif service_name == 'ByBit':
            conn_to_bybit_public(account)
            task_start_bybit_sender = asyncio.create_task(bybit_sender(account.id, account.name, _queue))
            conn_to_bybit_private(account)
            bybit_sub_to_position_stream(account.name)
            bybit_sub_to_order_stream(account.name)

            TASK_DICT[account].append(task_start_bybit_sender)

        # Создаем объект в бд
        ws_id = await create_conn_account_to_db(account)
        await add_to_ws_ids_dict(ws_id, service_name, account)


async def close_connection(account):
    service_name = await account.service_name
    tasks = TASK_DICT[account]
    for task in tasks:
        task.cancel()

    if service_name == 'Binance':
        client = binance_clients[account.name]
        await client.close_connection()

    elif service_name == 'ByBit':
        client = bybit_ws_private[account.name]
        client.exit()


async def ws_conn_check():
    while True:
        with ws_locker:
            with binance_ws_locker:
                for client in binance_clients.values():
                    try:
                        await client.get_server_time()
                    except:
                        ws_id = ws_ids[conn]
                        await update_wsmanager_status(ws_id, False)

            with bybit_ws_locker:
                for conn in bybit_ws_private.values():
                    if not conn.is_connected():
                        ws_id = ws_ids[conn]
                        await update_wsmanager_status(ws_id, False)

        time.sleep(10)


async def main():
    if not len(TASK_DICT):
        logging.basicConfig(level=logging.INFO)
        # task_start_server = asyncio.create_task(start_server())
        # task_ws_conn_check = asyncio.create_task(ws_conn_check())

        # Запуск бесконечных функций
        task_start_server = start_server()
        task_ws_conn_check = ws_conn_check()

        accounts = await get_accounts()

        for account in accounts.values():
            await new_connect(account)
            await asyncio.sleep(1)

        await asyncio.gather(task_start_server, task_ws_conn_check)


if __name__ == "__main__":
    asyncio.run(main())
