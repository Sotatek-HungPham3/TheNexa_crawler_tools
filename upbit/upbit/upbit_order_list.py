import traceback

import jwt
import hashlib
import requests
import uuid
from urllib.parse import urlencode, unquote

access_key = "nHPmoSxSQuyxAvnkx7x3fgWB3VQJcQ0Rz31pEiZx"
secret_key = "U7ihxL0s4JtuIM4eYGfRQ1Yvrm1XQcL9yUnix2CP"
server_url = 'https://api.upbit.com'


def get_all_order(state='wait', page=1, limit=100, contain_req=False):
    params = {
        'states[]': state,
        'page': page,
        'limit': limit
    }
    query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

    m = hashlib.sha512()
    m.update(query_string)
    query_hash = m.hexdigest()

    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
        'query_hash': query_hash,
        'query_hash_alg': 'SHA512',
    }

    jwt_token = jwt.encode(payload, secret_key)
    authorization = 'Bearer {}'.format(jwt_token)
    headers = {
        'Authorization': authorization,
    }
    try:
        orders = requests.get(server_url + '/v1/orders', params=params, headers=headers)
        return orders.json()
    except Exception as x:
        print(traceback.format_exc())
        return None