import datetime
import json
import os
import time

import pandas as pd
import requests


def download_csv(table_code, storage, from_year=2000):
    from_date = datetime.date(from_year, 1, 1)
    to_date = datetime.date.today()

    from_unixtime = int(time.mktime(from_date.timetuple()))
    to_unixtime = int(time.mktime(to_date.timetuple()))

    url = 'https://query1.finance.yahoo.com/v7/finance/chart/' \
        + table_code.upper() + '?' \
        + f'period1={from_unixtime}' \
        + f'&period2={to_unixtime}' \
        + '&interval=1d&indicators=quote&includeTimestamps=true'

    response = requests.get(url)
    assert response.status_code == 200
    json_response = response.json()

    result = json_response['chart']['result'][0]
    timestamp = result['timestamp']
    quote = result['indicators']['quote'][0]

    date = [
        f'{datetime.datetime.fromtimestamp(t):%Y-%m-%d}' for t in timestamp]

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
    for year in range(from_year, to_date.year + 1):
        filename = f'{table_code}-{year}-yahoofinance.csv'
        print(filename)
        path = os.path.join(storage, filename)
        idx = df['Date'].apply(lambda x: x.startswith(str(year)))
        if idx.any():
            with open(path, 'w') as f:
                f.write(df.loc[idx, ].to_csv(index=False))


def main(table_code, from_year, storage):
    download_csv(table_code, storage, from_year)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Yahoo Finance utility")

    parser.add_argument("--table-code", required=True,
                        help="The dataset table code")
    parser.add_argument("--from-year", required=True,
                        type=int, help="The first year to download")
    parser.add_argument("--storage", required=True,
                        help="The path were the files will be downloaded to")

    args = parser.parse_args()
    main(args.table_code, args.from_year, args.storage)
