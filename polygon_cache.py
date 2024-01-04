import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal
from lumibot import LUMIBOT_CACHE_FOLDER
from lumibot.entities import Asset
from lumibot.tools.polygon_helper import build_cache_filename, load_cache, get_missing_dates, get_polygon_symbol

# noinspection PyPackageRequirements
from polygon import RESTClient
from tqdm import tqdm

WAIT_TIME = 60
POLYGON_QUERY_COUNT = 0  # This is a variable that updates every time we query Polygon
MAX_POLYGON_DAYS = 30

# Modified get_price_data_from_polygon function that only gets data from cached files
def get_price_data_from_polygon_cache(
    api_key: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timespan: str = "minute",
    has_paid_subscription: bool = False,
    quote_asset: Asset = None,
):
    """
    Queries Polygon.io for pricing data for the given asset and returns a DataFrame with the data. Data will be
    cached in the LUMIBOT_CACHE_FOLDER/polygon folder so that it can be reused later and we don't have to query
    Polygon.io every time we run a backtest.

    Parameters
    ----------
    api_key : str
        The API key for Polygon.io
    asset : Asset
        The asset we are getting data for
    start : datetime
        The start date/time for the data we want
    end : datetime
        The end date/time for the data we want
    timespan : str
        The timespan for the data we want. Default is "minute" but can also be "second", "hour", "day", "week",
        "month", "quarter"
    has_paid_subscription : bool
        Set to True if you have a paid subscription to Polygon.io. This will prevent the script from waiting 1 minute
        between requests to avoid hitting the rate limit.
    quote_asset : Asset
        The quote asset for the asset we are getting data for. This is only needed for Forex assets.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the pricing data for the asset

    """
    global POLYGON_QUERY_COUNT  # Track if we need to wait between requests

    # Check if we already have data for this asset in the feather file
    df_all = None
    df_feather = None
    cache_file = build_cache_filename(asset, timespan)
    if cache_file.exists():
        print(f"\nLoading pricing data for {asset} / {quote_asset} with '{timespan}' timespan from cache file...")
        df_feather = load_cache(cache_file)
        df_all = df_feather.copy()  # Make a copy so we can check the original later for differences

    # Check if we need to get more data
    missing_dates = get_missing_dates(df_all, asset, start, end)
    if not missing_dates:
        return df_all

    # Return data that we have.
    return df_feather if df_feather is not None else None
