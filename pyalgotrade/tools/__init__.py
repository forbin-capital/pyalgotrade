import os

import six

import pyalgotrade
from pyalgotrade import bar
from pyalgotrade.barfeed.feed import Feed
from pyalgotrade.constants import (DATASOURCE_COINMARKETCAP,
                                   DATASOURCE_YAHOOFINANCE)
from pyalgotrade.tools import coinmarketcap, yahoofinance
from pyalgotrade.utils.collections import get


def build_feed(instruments, fromYear, toYear, storage, frequency=bar.Frequency.DAY, timezone=None,
               skipErrors=False, authToken=None, columnNames={}, forceDownload=False,
               skipMalformedBars=False):
    """Build and load a :class:`pyalgotrade.barfeed.coinmarketcapfeed.Feed` using CSV files downloaded from Quandl.
    CSV files are downloaded if they haven't been downloaded before.

    :param sourceCode: The dataset source code.
    :type sourceCode: string.
    :param tableCodes: The dataset table codes.
    :type tableCodes: list.
    :param fromYear: The first year.
    :type fromYear: int.
    :param toYear: The last year.
    :type toYear: int.
    :param storage: The path were the files will be loaded from, or downloaded to.
    :type storage: string.
    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** or **pyalgotrade.bar.Frequency.WEEK**
        are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param skipErrors: True to keep on loading/downloading files in case of errors.
    :type skipErrors: boolean.
    :param authToken: Optional. An authentication token needed if you're doing more than 50 calls per day.
    :type authToken: string.
    :param columnNames: Optional. A dictionary to map column names. Valid key values are:

        * datetime
        * open
        * high
        * low
        * close
        * volume
        * adj_close

    :type columnNames: dict.
    :param skipMalformedBars: True to skip errors while parsing bars.
    :type skipMalformedBars: boolean.

    :rtype: :class:`pyalgotrade.barfeed.coinmarketcapfeed.Feed`.
    """

    logger = pyalgotrade.logger.getLogger("data")
    ret = Feed(frequency, timezone)

    # Additional column names.
    for col, name in six.iteritems(columnNames):
        ret.setColumnName(col, name)

    if not os.path.exists(storage):
        logger.info("Creating %s directory" % (storage))
        os.mkdir(storage)

    for year in range(fromYear, toYear + 1):
        for instrument in instruments:
            datasource = instrument.datasource()
            fileName = os.path.join(
                storage, f'{instrument}-{year}-{datasource}.csv')
            if not os.path.exists(fileName) or forceDownload:
                logger.info("Downloading %s %d to %s" %
                            (instrument, year, fileName))
                try:
                    assert frequency == bar.Frequency.DAY, "Invalid frequency"
                    if datasource == DATASOURCE_COINMARKETCAP:
                        coinmarketcap.download_daily_bars(
                            instrument, year, fileName, authToken)
                    else:
                        assert datasource == DATASOURCE_YAHOOFINANCE, "Invalid data source"
                        yahoofinance.download_daily_bars(
                            instrument, year, fileName, authToken)
                except Exception as e:
                    if skipErrors:
                        logger.error(str(e))
                        continue
                    else:
                        raise e
            ret.addBarsFromCSV(instrument, fileName,
                               skipMalformedBars=skipMalformedBars)
    return ret
