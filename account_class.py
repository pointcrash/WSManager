from db import get_service_name


class Account:
    def __init__(self, data):
        self.id = data['id']
        self.name = data['name']
        self.service_id = data['service_id']
        self.key = data['API_TOKEN']
        self.secret = data['SECRET_KEY']
        self.testnet = not data['is_mainnet']
        self.demo_net = data['is_demonet']
        self.account_type = data['account_type']

    @property
    async def service_name(self):
        return await get_service_name(self.service_id)

