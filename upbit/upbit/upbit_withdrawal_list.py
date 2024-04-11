import traceback

import jwt
import hashlib
import requests
import uuid
from urllib.parse import urlencode, unquote


access_key = "sluc5eaSvJ4zE0I2pBYgxaOxlP8wEGYUM39qMwJu"
secret_key = "EWG4D4xUyXSCcqbPXzUbTT8cEy0Ef5MxlxjkqeMq"

# access_key = "eNCusCiGJaT6aL7UxUX3pj44x1c4PNE2TJQoHLgW"
# secret_key = "4a250hCt31P9lKlkT13j9ylCwrA7Jopcb21OZoyA"
server_url = 'https://api.upbit.com'


def get_all_withdrawal(state='WAITING', page=1, limit=100, contain_req=False):
    params = {
        'state': state,
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
        deposits = requests.get(server_url + '/v1/withdraws', params=params, headers=headers)
        return deposits.json()
    except Exception as x:
        print(traceback.format_exc())
        return None