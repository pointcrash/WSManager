from format import get_account
from global_variables import account_names, TASK_DICT


async def conn_account_check(acc_id):
    async def get_account_from_list(acc_id):
        for account in TASK_DICT:
            if account.id == acc_id:
                return account

    account = await get_account_from_list(acc_id)
