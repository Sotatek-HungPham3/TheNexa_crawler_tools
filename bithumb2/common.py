def set_key_trade(search, transfer_date, order_currency, payment_currency, order_balance, payment_balance):
    return 'key_' + order_currency + '_' + payment_currency + '_' + transfer_date + '_' + search + '_' + order_balance + '_' + payment_balance


def check_duplicate(key, lstKeys):
    if key in lstKeys:
        return False

    return True
