from pyalgotrade.constants import (DATASOURCE_COINMARKETCAP,
                                   DATASOURCE_YAHOOFINANCE, EXCHANGE_NYSE_ARCA)


class Instrument():

    def __init__(self, symbol, exchange):
        self._symbol = symbol
        self._exchange = exchange

    def timezone(self):
        if self._exchange == EXCHANGE_NYSE_ARCA:
            return 'US/Eastern'
        return 'UTC'

    def symbol(self):
        return self._symbol

    def exchange(self):
        return self._exchange

    def datasource(self):
        if self._exchange in [EXCHANGE_NYSE_ARCA]:
            return DATASOURCE_YAHOOFINANCE
        else:
            return DATASOURCE_COINMARKETCAP

    def __str__(self):
        return f'{self._symbol},{self._exchange}'
