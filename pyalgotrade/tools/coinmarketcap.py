import argparse
import datetime
import os
import time
from urllib.error import HTTPError

import pandas as pd
import requests
import six
from bs4 import BeautifulSoup

import pyalgotrade.logger
from pyalgotrade import bar
from pyalgotrade.barfeed import coinmarketcapfeed
from pyalgotrade.utils import csvutils, dt


def read_html(url):
    while True:
        try:
            df = pd.read_html(url)
            return df
        except HTTPError as err:
            if err.code == 429:
                print('waiting...')
                time.sleep(60)


def download_csv(instrument, begin, end, frequency, authToken):
    all_cryptocurrencies = list_all_cryptocurrencies()
    idx = all_cryptocurrencies.loc[:, 'Symbol'] == instrument.symbol()
    url_code = all_cryptocurrencies.loc[idx, 'Url Code'].values[0]
    url = "https://coinmarketcap.com/currencies/%s/historical-data/?start=%s&end=%s" % (
        url_code, begin.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    try:
        all_df = read_html(url)
    except ValueError:
        return ''
    main_df = [
        df for df in all_df if df.shape[0] > 0 and df.shape[1] == 7]
    assert len(main_df) == 1
    raw_df = main_df[0]
    raw_df['Date'] = raw_df['Date'].apply(lambda x: datetime.datetime.strptime(
        x, '%b %d, %Y'))
    df = raw_df[(raw_df['Date'] >= pd.Timestamp(begin))
                & (raw_df['Date'] <= pd.Timestamp(end))]
    df.loc[:, 'Date'] = df['Date'].apply(
        lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))
    df = df.rename(columns={'Open*': 'Open', 'Close**': 'Close'})
    df['Adj Close'] = df['Close']
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


def list_all_cryptocurrencies():
    path = os.path.join('/tmp', 'all-cryptocurrencies.json')
    if os.path.exists(path):
        return pd.read_json(path, orient='records')

    url = 'https://coinmarketcap.com/all/views/all/'
    all_df = read_html(url)
    main_df = [
        df for df in all_df if df.shape[0] > 30 and df.shape[1] == 11]
    assert len(main_df) == 1
    df = main_df[0]
    df = df.loc[:, ~df.columns.str.contains('(^Unnamed)|#')]
    df.loc[:, 'Name'] = df['Name'].apply(lambda x: x[1 + x.find(' '):].strip())

    headers = requests.utils.default_headers()
    headers.update(
        {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'})
    req = requests.get(url, headers)
    soup = BeautifulSoup(req.content, 'html.parser')
    hrefs = [a['href'] for a in soup.find_all(
        'a', {'class': 'currency-name-container'}, href=True)]
    assert len(hrefs) == df.shape[0]
    df.loc[:, 'Url Code'] = [h.split('/')[2] for h in hrefs]
    df = df.loc[:, ['Name', 'Symbol', 'Url Code']]
    with open(path, 'w') as f:
        df.to_json(f, orient='records')
    return df
