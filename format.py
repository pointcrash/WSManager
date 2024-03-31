import asyncio

from account_class import Account
from db import fetch_accounts
from global_variables import account_names, bybit_interval_list, binance_interval_list


async def get_account(pk):
    return Account((await fetch_accounts(pk))[0])


async def get_accounts():
    accounts = await fetch_accounts()
    formatted_data = dict()
    for account in accounts:
        account_names.append(account['name'])
        formatted_data[account['name']] = Account(account)
    return formatted_data

'''
    returned data: 
        {"name": AccountClass} 
'''


def format_binance_position_message(msg):
    formatted_msg_list = []
    position_list = msg['a']['P']
    for p in position_list:
        formatted_message = {
            'topic': 'position',
            'symbol': p['s'],
            'qty': p['pa'],
            'entryPrice': p['ep'],
            'unrealisedPnl': p['up'],
            'realisedPnl': p['cr'],
            'side': p['ps'],
        }
        formatted_msg_list.append(formatted_message)
    return formatted_msg_list


def format_binance_order_message(msg):
    msg = msg['o']
    formatted_message = {
        'topic': 'order',
        'symbol': msg['s'],
        'orderId': msg['i'],
        'clientOrderId': msg['c'],
        'side': msg['S'],
        'qty': msg['q'],
        'avgPrice': msg['ap'],
        'triggerPrice': msg['sp'],
        'triggerDirection': msg['ot'],
        'status': msg['X'],
        'psnSide': msg['ps'],
        'reduceOnly': msg['R'],
    }
    return formatted_message


def format_bybit_position_message(msg):
    formatted_message = {
        'topic': 'position',
        'symbol': msg['symbol'],
        'qty': msg['size'],
        'entryPrice': msg['entryPrice'],
        'unrealisedPnl': msg['unrealisedPnl'],
        'realisedPnl': msg['curRealisedPnl'],
        # 'side': 'LONG' if msg['side'] == 'Buy' else 'SHORT',
    }
    if msg['side'] == 'Buy':
        formatted_message['side'] = 'LONG'
    elif msg['side'] == 'Sell':
        formatted_message['side'] = 'SHORT'
    else:
        formatted_message['side'] = ''

    return formatted_message


def format_bybit_order_message(msg):
    formatted_message = {
        'topic': 'order',
        'symbol': msg['symbol'],
        'orderId': msg['orderId'],
        'clientOrderId': msg['orderLinkId'],
        'side': msg['side'].upper(),
        'qty': msg['qty'],
        'avgPrice': msg['price'],
        'triggerPrice': msg['triggerPrice'],
        'triggerDirection': msg['triggerDirection'],
        'status': msg['orderStatus'],
        'psnSide': 'LONG' if msg['positionIdx'] == 1 else 'SHORT',
        'reduceOnly': msg['reduceOnly'],
    }
    return formatted_message


def format_kline_interval_to_binance(interval):
    interval = str(interval)
    if interval in bybit_interval_list:
        index = bybit_interval_list.index(interval)
        return binance_interval_list[index]
    else:
        raise ValueError(f'Invalid interval. Interval {interval} not in list')


def format_kline_interval_to_numbers(interval):
    interval = str(interval)
    if interval in binance_interval_list:
        index = binance_interval_list.index(interval)
        return bybit_interval_list[index]
    else:
        raise ValueError(f'Invalid interval. Interval {interval} not in list')



