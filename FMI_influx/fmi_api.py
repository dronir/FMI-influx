import aiohttp
import logging

from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from .config import get_config


QUERY_URL = "https://opendata.fmi.fi/wfs"

BASE_QUERY_PARAMS = {
    "service": "WFS",
    "storedquery_id": "fmi::observations::weather::simple",
    "request": "getFeature",
    "version": "2.0.0",
}


def get_timestring(t: datetime) -> str:
    """ISO time string without decimals, end in Z, from datetime object."""
    return t.isoformat().split(".")[0] + "Z"


def get_query_params() -> Dict[str, Any]:
    config = get_config()

    now = datetime.utcnow()
    start = now - timedelta(minutes=config.FMI.history)

    query_params = BASE_QUERY_PARAMS.copy()
    query_params["place"] = config.FMI.location
    query_params["parameters"] = ",".join(config.FMI.variables)
    query_params["starttime"] = get_timestring(start)
    query_params["endtime"] = get_timestring(now)
    return query_params


async def make_query() -> Optional[str]:
    """Make HTTP request, return text contents."""
    logging.info(f"Getting data from {QUERY_URL}")
    query_params = get_query_params()
    logging.debug(f"Query parameters: {query_params}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(QUERY_URL, params=query_params) as response:
                logging.debug(response)
                if response.status == 200:
                    return await response.text()
                else:
                    logging.error(f"HTTP error {response.status} when fetching data.")
                    return None
    except aiohttp.ClientConnectorError as E:
        logging.error(f"Error while retrieving data: {E}")
        return None
    except Exception as E:
        logging.error(f"Unexpected exception: {E}")
        return None
