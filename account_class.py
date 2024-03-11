from db import get_service_name


class Account:
    def __init__(self, data):
        self.name = data['name']
        self.service_id = data['service_id']
        self.key = data['API_TOKEN']
        self.secret = data['SECRET_KEY']
        self.testnet = not data['is_mainnet']

    @property
    async def service_name(self):
        return await get_service_name(self.service_id)

