import traceback

import jwt
import hashlib
import requests
import uuid
from urllib.parse import urlencode, unquote

access_key = "2LAnWbMNmcxobMk9tZqWOuaxKbRtC2nr9hRG0uHk"
secret_key = "VanvxQMggLVeXM3kgyH7T6vL0gFDZ4xb423xxt3y"
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