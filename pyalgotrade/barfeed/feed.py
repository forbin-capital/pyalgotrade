from pyalgotrade import bar
from pyalgotrade.barfeed import coinmarketcapfeed, csvfeed, yahoofeed
from pyalgotrade.constants import (DATASOURCE_COINMARKETCAP,
                                   DATASOURCE_YAHOOFINANCE)


class Feed(csvfeed.BarFeed):
    """A :class:`pyalgotrade.barfeed.csvfeed.BarFeed` that loads bars from CSV files downloaded from Google Finance.

    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** is currently supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        Google Finance csv files lack timezone information.
        When working with multiple instruments:

            * If all the instruments loaded are in the same timezone, then the timezone parameter may not be specified.
            * If any of the instruments loaded are in different timezones, then the timezone parameter must be set.
    """

    def __init__(self, frequency=bar.Frequency.DAY, timezone=None, maxLen=None):
        if frequency not in [bar.Frequency.DAY]:
            raise Exception("Invalid frequency.")

        super(Feed, self).__init__(frequency, maxLen)

        self.__timezone = timezone
        self.__sanitizeBars = False

    def sanitizeBars(self, sanitize):
        self.__sanitizeBars = sanitize

    def barsHaveAdjClose(self):
        return True

    def addBarsFromCSV(self, instrument, path, timezone=None, skipMalformedBars=False):
        """Loads bars for a given instrument from a CSV formatted file.
        The instrument gets registered in the bar feed.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param path: The path to the CSV file.
        :type path: string.
        :param timezone: The timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
        :type timezone: A pytz timezone.
        :param skipMalformedBars: True to skip errors while parsing bars.
        :type skipMalformedBars: boolean.
        """

        if timezone is None:
            timezone = self.__timezone

        datasource = instrument.datasource()

        if datasource == DATASOURCE_YAHOOFINANCE:
            rowParser = yahoofeed.RowParser(self.getDailyBarTime(
            ), self.getFrequency(), timezone, self.__sanitizeBars)
        elif datasource == DATASOURCE_COINMARKETCAP:
            rowParser = coinmarketcapfeed.RowParser(self.getDailyBarTime(
            ), self.getFrequency(), timezone, self.__sanitizeBars)
        else:
            rowParser = csvfeed.GenericRowParser(
                self.__columnNames, self.__dateTimeFormat, self.getDailyBarTime(), self.getFrequency(),
                timezone, self.__barClass
            )

        super(Feed, self).addBarsFromCSV(instrument, path,
                                         rowParser, skipMalformedBars=skipMalformedBars)
