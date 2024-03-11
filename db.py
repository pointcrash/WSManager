import asyncio

import asyncpg
from asyncpg import Connection


async def connect_to_db():
    conn: Connection = await asyncpg.connect(
        user="admin",
        password="74976",
        database="BTC_USDT_bot_db",
        host="localhost",
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
