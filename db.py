import asyncio
import traceback

import asyncpg
from asyncpg import Connection

from global_variables import binance_clients, bybit_ws_private, ws_ids


# async def connect_to_db():
#     conn: Connection = await asyncpg.connect(
#         user="admin",
#         password="74976",
#         database="BTC_USDT_bot_db",
#         host="localhost",
#         port="5432",
#     )
#     return conn


async def connect_to_db():
    conn: Connection = await asyncpg.connect(
        user="admin",
        password="lksd23GBKwed.",
        database="MyDB",
        host="db",
        port="5432",
    )
    return conn


async def fetch_accounts(pk=None):
    conn = await connect_to_db()
    promt = "SELECT * FROM main_account"
    try:
        if pk:
            promt += f" WHERE id = {pk}"
        accounts = await conn.fetch(promt)
        return accounts
    finally:
        await conn.close()


async def fetch_services(pk=None):
    conn = await connect_to_db()
    promt = "SELECT * FROM main_ExchangeService"
    try:
        if pk:
            promt += f" WHERE id = {pk}"
        services = await conn.fetch(promt)
        return services
    finally:
        await conn.close()


async def get_service_name(pk):
    service = await fetch_services(pk)
    try:
        return service[0]['name']
    except Exception as e:
        pass


async def save_to_db(acc_id, data):
    conn = await connect_to_db()
    try:
        if data['topic'] == 'position':
            await save_position(conn, acc_id, data)
        elif data['topic'] == 'order':
            await save_order(conn, acc_id, data)
        else:
            return
    except Exception as e:
        return traceback.format_exc()
    finally:
        await conn.close()


async def save_order(conn, acc_id, data):
    try:
        query = """
        INSERT INTO orders_order (
            account_id, symbol_name, order_id, client_order_id,
            side, qty, price, avg_price, trigger_price, trigger_direction,
            status, psn_side, reduce_only, time_create, time_update
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW()
        )
        """
        values = (
            acc_id, data['symbol'], str(data['orderId']), data['clientOrderId'], data['side'],
            data['qty'], data['price'], data['avgPrice'], data['triggerPrice'],
            str(data['triggerDirection']), data['status'], data['psnSide'], data['reduceOnly']
        )
        await conn.fetchval(query, *values)
    finally:
        await conn.close()


async def save_position(conn, acc_id, data):
    try:
        query = """
        INSERT INTO orders_position (
            account_id, symbol_name, side, qty, entry_price,
            unrealised_pnl, realised_pnl, time_create, time_update
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, NOW(), NOW()
        )
        """
        values = (
            acc_id, data['symbol'], data['side'], data['qty'],
            data['entryPrice'], data['unrealisedPnl'], data['realisedPnl']
        )
        await conn.fetchval(query, *values)
    finally:
        await conn.close()


async def create_conn_account_to_db(account):
    conn = await connect_to_db()
    try:
        query = """
        INSERT INTO main_wsmanager (
            account_id, status, time_create, time_update
        ) VALUES (
            $1, $2, NOW(), NOW()
        )
        ON CONFLICT (account_id) 
        DO NOTHING
        RETURNING id
        """

        values = (account.id, True)
        ws_id = await conn.fetchval(query, *values)
        return ws_id
    finally:
        await conn.close()


async def add_to_ws_ids_dict(ws_id, service_name, account):
    conn = None
    if service_name == 'Binance':
        conn = binance_clients[account.name]
    elif service_name == 'ByBit':
        conn = bybit_ws_private[account.name]

    if conn:
        ws_ids[conn] = ws_id


async def update_wsmanager_status(ws_id, new_status):
    conn = await connect_to_db()
    try:
        query = """
        UPDATE main_wsmanager
        SET status = $1, time_update = NOW()
        WHERE id = $2
        """
        values = (new_status, ws_id)
        await conn.execute(query, *values)
    finally:
        await conn.close()

# Запуск асинхронного приложения

# <Record
# id=6 name='My Bibnance Account'
# API_TOKEN='40804baa38ed8e089157f32bee8c2311b0745b611b1dfb65ddfeda95af7f3b6b'
# SECRET_KEY='cd843d65f675cc9b3619871733f8d1c8b26a63a729ddcaabf4caba1fe973bbec'
# is_mainnet=False
# url='https://api-testnet.bybit.com'
# owner_id=4
# account_type='CONTRACT'
# service_id=1>
