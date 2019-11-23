import argparse
import datetime
import os

import pandas as pd
import requests
import six

import pyalgotrade.logger
from pyalgotrade import bar
from pyalgotrade.barfeed import coinmarketcapfeed
from pyalgotrade.utils import csvutils, dt

# http://www.coinmarketcap.com/help/api


def download_csv(sourceCode, tableCode, begin, end, frequency, authToken):
    url = "https://coinmarketcap.com/%s/%s/historical-data/?start=%s&end=%s" % (
        sourceCode, tableCode, begin.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    all_df = pd.read_html(url)
    historical_df = [
        df for df in all_df if df.shape[0] > 30 and df.shape[1] == 7]
    assert len(historical_df) == 1
    df = historical_df[0]
    df['Date'] = df['Date'].apply(lambda x: datetime.datetime.strptime(
        x, '%b %d, %Y').strftime('%Y-%m-%d'))
    df = df.rename(columns={'Open*': 'Open', 'Close**': 'Close'})
    data = df.to_csv(index=False)
    print(data)
    return data


def download_daily_bars(sourceCode, tableCode, year, csvFile, authToken=None):
    """Download daily bars from Quandl for a given year.

    :param sourceCode: The dataset's source code.
    :type sourceCode: string.
    :param tableCode: The dataset's table code.
    :type tableCode: string.
    :param year: The year.
    :type year: int.
    :param csvFile: The path to the CSV file to write.
    :type csvFile: string.
    :param authToken: Optional. An authentication token needed if you're doing more than 50 calls per day.
    :type authToken: string.
    """

    bars = download_csv(sourceCode, tableCode, datetime.date(
        year, 1, 1), datetime.date(year, 12, 31), "daily", authToken)
    f = open(csvFile, "w")
    f.write(bars)
    f.close()


def download_weekly_bars(sourceCode, tableCode, year, csvFile, authToken=None):
    """Download weekly bars from Quandl for a given year.

    :param sourceCode: The dataset's source code.
    :type sourceCode: string.
    :param tableCode: The dataset's table code.
    :type tableCode: string.
    :param year: The year.
    :type year: int.
    :param csvFile: The path to the CSV file to write.
    :type csvFile: string.
    :param authToken: Optional. An authentication token needed if you're doing more than 50 calls per day.
    :type authToken: string.
    """

    begin = dt.get_first_monday(
        year) - datetime.timedelta(days=1)  # Start on a sunday
    end = dt.get_last_monday(
        year) - datetime.timedelta(days=1)  # Start on a sunday
    bars = download_csv(sourceCode, tableCode, begin, end, "weekly", authToken)
    f = open(csvFile, "w")
    f.write(bars)
    f.close()


def build_feed(sourceCode, tableCodes, fromYear, toYear, storage, frequency=bar.Frequency.DAY, timezone=None,
               skipErrors=False, authToken=None, columnNames={}, forceDownload=False,
               skipMalformedBars=False
               ):
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

    logger = pyalgotrade.logger.getLogger("coinmarketcap")
    ret = coinmarketcapfeed.Feed(frequency, timezone)

    # Additional column names.
    for col, name in six.iteritems(columnNames):
        ret.setColumnName(col, name)

    if not os.path.exists(storage):
        logger.info("Creating %s directory" % (storage))
        os.mkdir(storage)

    for year in range(fromYear, toYear + 1):
        for tableCode in tableCodes:
            fileName = os.path.join(
                storage, "%s-%s-%d-coinmarketcap.csv" % (sourceCode, tableCode, year))
            if not os.path.exists(fileName) or forceDownload:
                logger.info("Downloading %s %d to %s" %
                            (tableCode, year, fileName))
                try:
                    if frequency == bar.Frequency.DAY:
                        download_daily_bars(
                            sourceCode, tableCode, year, fileName, authToken)
                    else:
                        assert frequency == bar.Frequency.WEEK, "Invalid frequency"
                        download_weekly_bars(
                            sourceCode, tableCode, year, fileName, authToken)
                except Exception as e:
                    if skipErrors:
                        logger.error(str(e))
                        continue
                    else:
                        raise e
            ret.addBarsFromCSV(tableCode, fileName,
                               skipMalformedBars=skipMalformedBars)
    return ret


def main():
    parser = argparse.ArgumentParser(description="Quandl utility")

    parser.add_argument("--auth-token", required=False,
                        help="An authentication token needed if you're doing more than 50 calls per day")
    parser.add_argument("--source-code", required=True,
                        help="The dataset source code")
    parser.add_argument("--table-code", required=True,
                        help="The dataset table code")
    parser.add_argument("--from-year", required=True,
                        type=int, help="The first year to download")
    parser.add_argument("--to-year", required=True, type=int,
                        help="The last year to download")
    parser.add_argument("--storage", required=True,
                        help="The path were the files will be downloaded to")
    parser.add_argument("--force-download", action='store_true',
                        help="Force downloading even if the files exist")
    parser.add_argument("--ignore-errors", action='store_true',
                        help="True to keep on downloading files in case of errors")
    parser.add_argument("--frequency", default="daily", choices=[
                        "daily", "weekly"], help="The frequency of the bars. Only daily or weekly are supported")

    args = parser.parse_args()

    logger = pyalgotrade.logger.getLogger("coinmarketcap")

    if not os.path.exists(args.storage):
        logger.info("Creating %s directory" % (args.storage))
        os.mkdir(args.storage)

    for year in range(args.from_year, args.to_year + 1):
        fileName = os.path.join(args.storage, "%s-%s-%d-coinmarketcap.csv" %
                                (args.source_code, args.table_code, year))
        if not os.path.exists(fileName) or args.force_download:
            logger.info("Downloading %s %d to %s" %
                        (args.table_code, year, fileName))
            try:
                if args.frequency == "daily":
                    download_daily_bars(
                        args.source_code, args.table_code, year, fileName, args.auth_token)
                else:
                    assert args.frequency == "weekly", "Invalid frequency"
                    download_weekly_bars(
                        args.source_code, args.table_code, year, fileName, args.auth_token)
            except Exception as e:
                if args.ignore_errors:
                    logger.error(str(e))
                    continue
                else:
                    raise


if __name__ == "__main__":
    main()
