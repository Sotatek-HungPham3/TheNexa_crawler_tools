import base64
import hashlib
import hmac
import json
import uuid
import httplib2

BASE_URL = 'https://api.coinone.co.kr'

class CoinOne:
    api_key = ""
    api_secret = ""

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret


    def get_encoded_payload(self, payload):
        payload['nonce'] = str(uuid.uuid4())

        dumped_json = json.dumps(payload)
        encoded_json = base64.b64encode(bytes(dumped_json, 'utf-8'))
        return encoded_json


    def get_signature(self, encoded_payload):
        signature = hmac.new(self.api_secret, encoded_payload, hashlib.sha512)
        return signature.hexdigest()


    def get_response(self, action, payload):
        url = '{}{}'.format(BASE_URL, action)
        payload['access_token'] = self.api_key

        encoded_payload = self.get_encoded_payload(payload)

        headers = {
            'Content-type': 'application/json',
            'X-COINONE-PAYLOAD': encoded_payload,
            'X-COINONE-SIGNATURE': self.get_signature(encoded_payload),
        }

        http = httplib2.Http()
        response, content = http.request(url, 'POST', headers=headers)
        return content