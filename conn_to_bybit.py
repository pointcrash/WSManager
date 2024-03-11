import json
import time
from uuid import uuid4

from pybit.unified_trading import WebSocket

from format import format_bybit_order_message, format_bybit_position_message
from global_variables import bybit_ws_private, bybit_ws_public, queue_dict


# API_TOKEN = 'XlXhlUPG4GCBGRdFld'
# SECRET_KEY = 'JBpwCjzkzXbxriLdptaoLyLR2wvdNSz0NisU'
# symbol = 'BTCUSDT'


def conn_to_bybit_public():
    if not len(bybit_ws_public):
        ws_public = WebSocket(
            # trace_logging=True,
            testnet=True,
            channel_type="linear",
        )

        bybit_ws_public.append(ws_public)


def position_handler_wrapper(_queue):
    def handler(msg):
        psn_list = msg['data']
        for psn in psn_list:
            msg = format_bybit_position_message(psn)
            _queue.put_nowait(msg)

    return handler


def order_handler_wrapper(_queue):
    def handler(msg):
        order_list = msg['data']
        for order in order_list:
            msg = format_bybit_order_message(order)
            _queue.put_nowait(msg)

    return handler


def mark_price_handler_wrapper(_queue, symbol):
    def handler(msg):
        formatted_msg = {'topic': 'markPrice', 'symbol': symbol, 'markPrice': msg['data']['markPrice']}
        _queue.put_nowait(formatted_msg)

    return handler


def kline_handler_wrapper(_queue, symbol):
    def handler(msg):
        if msg['data'][0]['confirm'] is True:
            formatted_msg = {'topic': 'kline',
                             'symbol': symbol,
                             'interval': msg['data'][0]['interval'],
                             'closePrice': msg['data'][0]['close'],
                             }
            _queue.put_nowait(formatted_msg)

    return handler


def conn_to_bybit_private(account):
    ws_private = WebSocket(
        # trace_logging=True,
        testnet=account.testnet,
        channel_type="private",
        api_key=account.key,
        api_secret=account.secret,
    )

    if ws_private.is_connected():
        print(f"WebSocket '{account.name}' connected")
    else:
        print(f"WebSocket '{account.name}' connection error")

    bybit_ws_private[account.name] = ws_private


def unsub_from_topic(splitted_topic):
    topic = '.'.join(splitted_topic)
    ws_public = bybit_ws_public[0]

    for req_id, data in list(ws_public.subscriptions.items()):
        data = json.loads(data)
        if data['args'][0] == topic:
            # req_id = str(uuid4())
            ws_public.unsub_topics[req_id] = topic
            unsubscribe_message = {"op": "unsubscribe", "req_id": req_id, "args": [topic]}
            ws_public.ws.send(json.dumps(unsubscribe_message))


def bybit_sub_to_mp(symbol, _queue):
    ws_public = bybit_ws_public[0]
    ws_public.ticker_stream(symbol=symbol, callback=mark_price_handler_wrapper(_queue, symbol))


def bybit_sub_to_kline(interval, symbol, _queue):
    ws_public = bybit_ws_public[0]
    ws_public.kline_stream(interval=interval, symbol=symbol, callback=kline_handler_wrapper(_queue, symbol))


def bybit_sub_to_position_stream(account_name):
    _queue = queue_dict[account_name]
    ws_private = bybit_ws_private[account_name]
    ws_private.position_stream(callback=position_handler_wrapper(_queue))


def bybit_sub_to_order_stream(account_name):
    _queue = queue_dict[account_name]
    ws_private = bybit_ws_private[account_name]
    ws_private.order_stream(callback=order_handler_wrapper(_queue))


def get_topic(topic, symbol, interval):
    if topic == 'tickers':
        return '.'.join([topic, symbol])
    elif topic == 'kline':
        return '.'.join([topic, interval, symbol])
    else:
        raise ValueError(f'Invalid topic {topic}')
