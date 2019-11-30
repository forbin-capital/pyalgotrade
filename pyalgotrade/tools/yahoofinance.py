import datetime
import json
import os
import time

import pandas as pd
import pytz
import requests


def download_csv(instrument, begin, end, frequency, authToken):
    from_unixtime = int(time.mktime(begin.timetuple()))
    to_unixtime = int(time.mktime(end.timetuple()))
    yf_symbol = instrument.symbol().split(':')[0].upper()

    url = 'https://query1.finance.yahoo.com/v7/finance/chart/' \
        + yf_symbol + '?' \
        + f'period1={from_unixtime}' \
        + f'&period2={to_unixtime}' \
        + '&interval=1d&indicators=quote&includeTimestamps=true'

    response = requests.get(url)
    assert response.status_code == 200
    json_response = response.json()

    result = json_response['chart']['result'][0]
    timestamp = result['timestamp']
    quote = result['indicators']['quote'][0]

    date = [pd.Timestamp(datetime.datetime.fromtimestamp(
        t), tz='US/Eastern') for t in timestamp]
    date = [d.tz_convert('UTC') for d in date]

    data = {
        'Date': date,
        'Open': quote['open'],
        'High': quote['high'],
        'Low': quote['low'],
        'Close': quote['close'],
        'Adj Close': result['indicators']['adjclose'][0]['adjclose'],
        'Volume': quote['volume']
    }
    df = pd.DataFrame(data)

    idx = (df['Date'] >= pd.Timestamp(begin, tz='UTC')) & (
        df['Date'] <= pd.Timestamp(end, tz='UTC'))
    df = df.loc[idx, :]

    df['Date'] = df['Date'].apply(lambda x: f'{x:%Y-%m-%d}')
    data = df.to_csv(index=False)
    return data


def download_daily_bars(instrument, year, csvFile, authToken=None):
    """Download daily bars from Quandl for a given year.

    :param symbol: The dataset's source code.
    :type symbol: string.
    :param exchange: The dataset's table code.
    :type exchange: string.
    :param year: The year.
    :type year: int.
    :param csvFile: The path to the CSV file to write.
    :type csvFile: string.
    :param authToken: Optional. An authentication token needed if you're doing more than 50 calls per day.
    :type authToken: string.
    """

    bars = download_csv(instrument, datetime.datetime(
        year, 1, 1, 23, 59, 59), datetime.datetime(year, 12, 31, 23, 59, 59), "daily", authToken)
    f = open(csvFile, "w")
    f.write(bars)
    f.close()
