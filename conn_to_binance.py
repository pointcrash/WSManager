from binance import AsyncClient, BinanceSocketManager

from format import format_binance_position_message, format_binance_order_message, format_kline_interval_to_numbers
from global_variables import binance_managers, queue_dict, binance_clients


# api_key = '40804baa38ed8e089157f32bee8c2311b0745b611b1dfb65ddfeda95af7f3b6b'
# api_secret = 'cd843d65f675cc9b3619871733f8d1c8b26a63a729ddcaabf4caba1fe973bbec'


async def connect_to_binance_client(account):
    client = await AsyncClient.create(api_key=account.key, api_secret=account.secret, testnet=account.testnet)
    bm = BinanceSocketManager(client)
    binance_clients[account.name] = client
    binance_managers[account.name] = bm
    return bm


async def sub_to_binance_user_topik(account_name):
    bm = binance_managers[account_name]
    _queue = queue_dict[account_name]
    ts = bm.futures_user_socket()
    async with ts as tscm:
        while True:
            message = await tscm.recv()
            if message['e'] == 'ACCOUNT_UPDATE':
                message_list = format_binance_position_message(message)
                for message in message_list:
                    await _queue.put(message)
            elif message['e'] == 'ORDER_TRADE_UPDATE':
                message = format_binance_order_message(message)
                await _queue.put(message)


async def sub_to_binance_market_price_topik(account_name, symbol, fast=False):
    bm = binance_managers[account_name]
    _queue = queue_dict[account_name]
    ts = bm.symbol_mark_price_socket(symbol=symbol, fast=fast)
    async with ts as tscm:
        while True:
            message = await tscm.recv()
            await _queue.put({
                'topic': 'markPrice',
                'symbol': symbol,
                'markPrice': message['data']['p'],
            })


async def sub_to_binance_kline_topik(account_name, symbol, interval):
    bm = binance_managers[account_name]
    _queue = queue_dict[account_name]
    ts = bm.kline_futures_socket(symbol=symbol, interval=interval)
    async with ts as tscm:
        while True:
            message = await tscm.recv()
            if message['k']['x'] is True:  # If kline is closed
                await _queue.put({
                    'topic': 'kline',
                    'symbol': symbol,
                    'interval': format_kline_interval_to_numbers(interval),
                    'closePrice': message['k']['c'],
                })
