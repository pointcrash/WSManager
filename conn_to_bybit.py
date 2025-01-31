import json
import logging
import threading
import time
from uuid import uuid4
from pybit.unified_trading import WebSocket, HTTP

from bybit_custom_ws_class import ByBitCustomWebSocket
from format import format_bybit_order_message, format_bybit_position_message
from global_variables import bybit_ws_private, bybit_ws_public, queue_dict


# API_TOKEN = 'XlXhlUPG4GCBGRdFld'
# SECRET_KEY = 'JBpwCjzkzXbxriLdptaoLyLR2wvdNSz0NisU'
# symbol = 'BTCUSDT'


def get_session(account):
    print(f"From get_session account data: {account.name, account.service_id, account.key, account.secret, account.testnet, account.demo_net, account.account_type} ")

    session = HTTP(
        testnet=False,
        demo=account.testnet,
        api_key=account.key,
        api_secret=account.secret,
    )

    return session


def bybit_api_check(account):
    try:
        session = get_session(account)
        session.get_wallet_balance(accountType=account.account_type)
        return True, ''
    except Exception as e:
        return False, str(e)


def conn_to_bybit_public(account):
    ws_public = ByBitCustomWebSocket(
        # trace_logging=True,
        testnet=account.testnet,
        channel_type="linear",
    )

    bybit_ws_public[account.name] = ws_public


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
    try:
        session = get_session(account)
        session.get_wallet_balance(accountType=account.account_type)
        ws_private = WebSocket(
            # trace_logging=True,
            testnet=account.testnet,
            channel_type="private",
            api_key=account.key,
            api_secret=account.secret,
        )

        if ws_private.is_connected():
            bybit_ws_private[account.name] = ws_private
            return True, ''
        else:
            return False, 'Not connected'
    except Exception as e:
        logging.info(f'{account.name} BYBIT CONN ERROR: {e}')
        return False, str(e)


def unsub_from_topic(account_name, splitted_topic):
    topic = '.'.join(splitted_topic)
    ws_public = bybit_ws_public[account_name]

    for req_id, data in list(ws_public.subscriptions.items()):
        data = json.loads(data)
        if data['args'][0] == topic:
            # req_id = str(uuid4())
            ws_public.unsubscribe_topics[req_id] = topic
            unsubscribe_message = {"op": "unsubscribe", "req_id": req_id, "args": [topic]}
            ws_public.ws.send(json.dumps(unsubscribe_message))


def bybit_sub_to_mp(account_name, symbol, _queue):
    ws_public = bybit_ws_public[account_name]
    ws_public.ticker_stream(symbol=symbol, callback=mark_price_handler_wrapper(_queue, symbol))


def bybit_sub_to_kline(account_name, interval, symbol, _queue):
    ws_public = bybit_ws_public[account_name]
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
