import asyncio
import websockets

from ciclic_queue import CyclicQueue
from conn_to_binance import *
from conn_to_bybit import *
from format import get_accounts, get_account, format_kline_interval_to_binance
from global_variables import active_connections, queue_dict, accounts_names


async def sub_to_kline(acc_name, service, symbol, interval):
    if service == 'ByBit':
        _queue = queue_dict[acc_name]
        bybit_sub_to_kline(interval, symbol, _queue)

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
        bybit_sub_to_mp(symbol, _queue)

    elif service == 'Binance':
        task = asyncio.create_task(sub_to_binance_market_price_topik(acc_name, symbol))
    return task


async def conn_handler(websocket, path):
    print("Client connected")
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

                if acc_name in accounts_names:
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
        # if acc_name:
        #     active_connections[acc_name].remove(websocket)
        # for task in tasks:
        #     task.cancel()
        # for sub in bybit_subs:
        #     unsub_from_topic(sub)
        print("Error - Client disconnected")
    except Exception as e:
        print(e)
    finally:
        if acc_name:
            active_connections[acc_name].remove(websocket)
        for task in tasks:
            task.cancel()
        for sub in bybit_subs:
            unsub_from_topic(sub)
        print("Connection closed")


async def binance_sender(acc_name, _queue):
    print(f"Sender '{acc_name}' started")
    while True:
        data = await _queue.get()
        print(f"'{acc_name}' sending data:", data)

        if active_connections.get(acc_name):
            for websocket in active_connections[acc_name]:
                try:
                    await websocket.send(json.dumps(data))
                except Exception as e:
                    print(e)


async def bybit_sender(acc_name, _queue):
    print(f"Sender '{acc_name}' started")
    while True:
        if _queue.qsize() > 0:
            data = _queue.get_nowait()
            print(f"'{acc_name}' sending data:", data)

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
    while True:
        if _queue.qsize() > 0:
            data = _queue.get_nowait()
            print(f"'{acc_name}' sending data:", data)

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
    server = await websockets.serve(conn_handler, "localhost", 8765)
    await server.wait_closed()


async def add_new_account(acc_pk):
    account = await get_account(acc_pk)
    if await account.service_name == 'Binance':
        _queue = asyncio.Queue()
        queue_dict[account.name] = _queue
        task_start_binance_sender = asyncio.create_task(binance_sender(account.name, _queue))
        await connect_to_binance_client(account)
        task_sub_binance_user = asyncio.create_task(sub_to_binance_user_topik(account.name))

    elif await account.service_name == 'ByBit':
        _queue = asyncio.Queue()
        queue_dict[account.name] = _queue
        task_start_bybit_sender = asyncio.create_task(bybit_sender(account.name, _queue))
        conn_to_bybit_private(account)
        bybit_sub_to_position_stream(account.name)
        bybit_sub_to_order_stream(account.name)


async def main():
    tasks_list = []
    task_start_server = asyncio.create_task(start_server())
    tasks_list.append(task_start_server)
    conn_to_bybit_public()
    accounts = await get_accounts()

    for account in accounts.values():
        if await account.service_name == 'Binance':
            _queue = asyncio.Queue()
            queue_dict[account.name] = _queue
            task_start_binance_sender = asyncio.create_task(binance_sender(account.name, _queue))
            await connect_to_binance_client(account)
            task_sub_binance_user = asyncio.create_task(sub_to_binance_user_topik(account.name))

            tasks_list.append(task_sub_binance_user)
            tasks_list.append(task_start_binance_sender)

        elif await account.service_name == 'ByBit':
            _queue = asyncio.Queue()
            queue_dict[account.name] = _queue
            task_start_bybit_sender = asyncio.create_task(bybit_sender(account.name, _queue))
            conn_to_bybit_private(account)
            bybit_sub_to_position_stream(account.name)
            bybit_sub_to_order_stream(account.name)

            tasks_list.append(task_start_bybit_sender)

        await asyncio.sleep(1)
    await asyncio.gather(*tasks_list)


if __name__ == "__main__":
    asyncio.run(main())
