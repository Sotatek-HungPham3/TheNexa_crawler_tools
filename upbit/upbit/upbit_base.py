import math
import jwt          # PyJWT
import re
import uuid
import hashlib
from urllib.parse import urlencode
from pyupbit.request_api import _send_get_request, _send_post_request, _send_delete_request


def get_all_order(state='wait', page=1, limit=100, contain_req=False):
    """
    주문 리스트 조회
    :param ticker: market
    :param state: 주문 상태(wait, watch, done, cancel)
    :param kind: 주문 유형(normal, watch)
    :param contain_req: Remaining-Req 포함여부
    :return:
    """
    try:
        url = "https://api.upbit.com/v1/orders"
        data = {
                'state': state,
                'page': page,
                'limit': limit,
                'order_by': 'desc'
                }
        headers = self._request_headers(data)
        result = _send_get_request(url, headers=headers, data=data)
        if contain_req:
            return result
        else:
            return result[0]
    except Exception as x:
        print(x)
        return None