import json
import threading
import time
from uuid import uuid4

import websocket

TESTNET_URL = "wss://stream-testnet.bybit.com/spot/public/v3"
MAINNET_URL = "wss://stream.bybit.com/spot/public/v3"


class MyWebSocket:

    def __init__(self, is_testnet=True):
        self.url = TESTNET_URL if is_testnet else MAINNET_URL
        self.topic_callback_list = dict()
        self.ws = None

    def _on_open(self, ws):
        print("WebSocket connected")

    def _on_message(self, ws, message):
        message = json.loads(message)
        try:
            if self.topic_callback_list.get(message['topic']):
                callback_func = self.topic_callback_list[message['topic']]
                callback_func(message)
        except:
            print(f"Received message: {message}")

    def _on_close(self, ws):
        print("WebSocket connection closed")

    def connect(self):
        self.ws = websocket.WebSocketApp(self.url,
                                         on_open=self._on_open,
                                         on_message=self._on_message,
                                         on_close=self._on_close)
        self.ws.run_forever()

    def sub_to_topic(self, topic, callback_func):
        self.topic_callback_list[topic] = callback_func

        req_id = str(uuid4())
        subscribe_message = {"op": "subscribe", "req_id": req_id, "args": [topic]}
        ws.ws.send(json.dumps(subscribe_message))

    def unsub_from_topic(self, topic):
        req_id = str(uuid4())

        print(req_id)
        unsubscribe_message = {"op": "unsubscribe", "req_id": req_id, "args": [topic]}
        ws.ws.send(json.dumps(unsubscribe_message))

    def send_message(self, message):
        if self.ws:
            self.ws.send(message)
        else:
            print("WebSocket is not connected")

    def close(self):
        if self.ws:
            self.ws.close()
        else:
            print("WebSocket is not connected")


def call_f(message):
    print(111, message)


# Пример использования
if __name__ == "__main__":
    ws = MyWebSocket()
    thread = threading.Thread(target=ws.connect)
    thread.start()

    time.sleep(2)
    ws.sub_to_topic('tickers.BTCUSDT', call_f)
    time.sleep(10)
    ws.unsub_from_topic('tickers.BTCUSDT')

    while True:
        print(1)
        time.sleep(2)

