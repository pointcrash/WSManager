import asyncio
import websockets
import logging

from ciclic_queue import CyclicQueue
from conn_to_binance import *
from conn_to_bybit import *
from format import get_accounts, get_account, format_kline_interval_to_binance
from global_variables import active_connections, queue_dict, account_names, TASK_DICT


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


async def binance_sender(acc_name, _queue):
    print(f"Sender '{acc_name}' started")
    logging.info(f"Sender '{acc_name}' started")
    while True:
        data = await _queue.get()
        # print(f"'{acc_name}' sending data:", data)

        if active_connections.get(acc_name):
            for websocket in active_connections[acc_name]:
                try:
                    await websocket.send(json.dumps(data))
                except Exception as e:
                    print(e)


async def bybit_sender(acc_name, _queue):
    print(f"Sender '{acc_name}' started")
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
                        print(e)
        else:
            await asyncio.sleep(1)


async def bybit_mark_price_sender(acc_name, _queue):
    print(f"Sender '{acc_name}' started")
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
                        print(e)
            await asyncio.sleep(2.9)
        else:
            await asyncio.sleep(3)


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
    service_name = await account.service_name
    _queue = asyncio.Queue()
    queue_dict[account.name] = _queue
    TASK_DICT[account] = []

    if service_name == 'Binance':
        task_start_binance_sender = asyncio.create_task(binance_sender(account.name, _queue))
        await connect_to_binance_client(account)
        task_sub_binance_user = asyncio.create_task(sub_to_binance_user_topik(account.name))

        TASK_DICT[account].append(task_sub_binance_user)
        TASK_DICT[account].append(task_start_binance_sender)

    elif service_name == 'ByBit':
        conn_to_bybit_public(account)
        task_start_bybit_sender = asyncio.create_task(bybit_sender(account.name, _queue))
        conn_to_bybit_private(account)
        bybit_sub_to_position_stream(account.name)
        bybit_sub_to_order_stream(account.name)

        TASK_DICT[account].append(task_start_bybit_sender)


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


async def main():
    if not len(TASK_DICT):
        logging.basicConfig(level=logging.INFO)
        task_start_server = asyncio.create_task(start_server())
        accounts = await get_accounts()

        for account in accounts.values():
            await new_connect(account)

            await asyncio.sleep(1)
        await asyncio.gather(task_start_server)


if __name__ == "__main__":
    asyncio.run(main())
