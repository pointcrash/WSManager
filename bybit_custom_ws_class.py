from pybit.unified_trading import WebSocket


'''Расширяем класс вебсокета из библиотеки для обработки
                                        сообщений об отписке'''


class ByBitCustomWebSocket(WebSocket):
    def __init__(self, channel_type, **kwargs):
        super().__init__(channel_type, **kwargs)
        self.unsubscribe_topics = dict()

    def _process_unsubscribe_message(self, message):
        req_id = message['req_id']
        topic = self.unsubscribe_topics[req_id]

        if message.get("success") is True:
            self.subscriptions.pop(req_id)
            self._pop_callback(topic=topic)

        # Futures unsubscribe fail
        elif message.get("success") is False:
            response = message["ret_msg"]
            print("Couldn't unsubscribe to topic." f"Error: {response}.")

    def _handle_incoming_message(self, message):
        def is_auth_message():
            if (
                message.get("op") == "auth"
                or message.get("type") == "AUTH_RESP"
            ):
                return True
            else:
                return False

        def is_subscription_message():
            if (
                message.get("op") == "subscribe"
                or message.get("type") == "COMMAND_RESP"
            ):
                return True
            else:
                return False

        def is_unsubscribe_message():
            if message.get("op") == "unsubscribe":
                return True
            else:
                return False

        if is_auth_message():
            self._process_auth_message(message)
        elif is_subscription_message():
            self._process_subscription_message(message)
        elif is_unsubscribe_message():
            self._process_unsubscription_message(message)
        else:
            self._process_normal_message(message)
