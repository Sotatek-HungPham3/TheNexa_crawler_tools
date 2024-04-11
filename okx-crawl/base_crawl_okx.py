import okx.Funding as Funding
import okx.Convert as Convert
import okx.Trade as Trade

class CrawlOkx:
    api_key = "6bd415fe-3ea4-41cf-91bd-ece5aaa303c9"
    secret_key = "530763B96E237AD3CA10611E0DC0BA73"
    passphrase = "Thanhthu@98"
    flag = "0"

    def crawl_deposit(self, start_time, end_time, limit):
        fundingApi = Funding.FundingAPI(self.api_key, self.secret_key, self.passphrase, False, self.flag)
        depositTxs = fundingApi.get_deposit_history(after=end_time, before=start_time, limit=limit)
        return depositTxs['data']

    def crawl_withdraw(self, start_time, end_time, limit):
        fundingApi = Funding.FundingAPI(self.api_key, self.secret_key, self.passphrase, False, self.flag)
        witdrawTxs = fundingApi.get_withdrawal_history(after=end_time, before=start_time, limit=limit)
        return witdrawTxs['data']

    def crawl_swap(self, start_time, end_time, limit):
        convertApi = Convert.ConvertAPI(self.api_key, self.secret_key, self.passphrase, False, self.flag)
        swapTxs = convertApi.get_convert_history(after=end_time, before=start_time, limit=limit)
        return swapTxs['data']

    def crawl_trade(self, type, before, limit, begin, end):
        tradeApi = Trade.TradeAPI(self.api_key, self.secret_key, self.passphrase, False, self.flag)
        tradeTxs = tradeApi.get_fills_history(instType=type, before=before, limit=limit, begin=begin, end=end)
        print(f'[TRADE ]', tradeTxs)

        return tradeTxs['data']

    def crawl_trade_last_2_years(self):
        tradeApi = Trade.TradeAPI(self.api_key, self.secret_key, self.passphrase, False, self.flag)
        tradeTxs = tradeApi.get_fills_history(instType=type)
        return tradeTxs['data']
