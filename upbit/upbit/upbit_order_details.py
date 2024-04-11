import time
import traceback

import pyupbit
import json


access_key = "eNCusCiGJaT6aL7UxUX3pj44x1c4PNE2TJQoHLgW"
secret_key = "4a250hCt31P9lKlkT13j9ylCwrA7Jopcb21OZoyA"
upbit = pyupbit.Upbit(access_key, secret_key)

try:
    # get order detail
    order_detail = upbit.get_order('df01a990-c121-44b4-8faa-59be5e792947')
    print(json.dumps(order_detail))
except Exception as x:
    print(traceback.format_exc())
