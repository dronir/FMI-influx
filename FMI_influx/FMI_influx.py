import aiohttp
import asyncio
import aioinflux
import logging

from functools import cache
from pydantic import BaseSettings, Field
from typing import Dict, Any, Tuple, List, Optional
from collections import defaultdict
from datetime import datetime, timedelta
from math import isnan
from lxml import etree

DEBUG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}

QUERY_URL = "https://opendata.fmi.fi/wfs"

BASE_QUERY_PARAMS = {
    "service": "WFS",
    "storedquery_id": "fmi::observations::weather::simple",
    "request": "getFeature",
    "version": "2.0.0",
}


class FMIConfig(BaseSettings):
    history: int = 60
    delay: int = 10
    variables: List[str] = ["temperature", "pressure"]
    location: str

    class Config:
        env_file = ".env"
        env_prefix = "UPLOADER_FMI_"


class InfluxConfig(BaseSettings):
    measurement: str
    host: str
    port: int = 8086
    database: str
    username: str
    password: str
    tags: Dict[str, str] = {}

    class Config:
        env_file = ".env"
        env_prefix = "UPLOADER_INFLUX_"


class Config(BaseSettings):
    debug_level: str = "info"
    FMI: FMIConfig = Field(default_factory=FMIConfig)
    influx: InfluxConfig = Field(default_factory=InfluxConfig)

    class Config:
        env_prefix = "UPLOADER_"


@cache
def get_config():
    return Config()


def get_timestring(t: datetime) -> str:
    """ISO time string without decimals, end in Z, from datetime object."""
    return t.isoformat().split(".")[0] + "Z"


async def rawdata_from_query() -> Optional[str]:
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


def xml_from_raw(raw_result: str) -> etree.Element:
    """Turn text content from HTTP request into XML tree."""
    xml_data = bytes(raw_result, "utf-8")
    try:
        return etree.fromstring(xml_data)
    except Exception as E:
        logging.error(f"Error while parsing XML data:\n {E}")
        return etree.Element("root")  # Return an empty XML tree.


def values_from_xml(xml_tree: etree.Element) -> List[Tuple]:
    """Return all (time, variable, value) triplets, given XML tree."""
    # Handle namespaces properly because why not (could just wildcard them to be honest)
    ns_wfs = xml_tree.nsmap["wfs"]
    ns_bswfs = xml_tree.nsmap["BsWfs"]
    members = xml_tree.findall(f".//{{{ns_wfs}}}member")
    return [value_from_element(m, ns_bswfs) for m in members]


def value_from_element(member: etree.Element, ns: str = "*") -> Tuple:
    """Get one (time, variable, value) triplet from the XML element containing it."""
    time = member.find(f".//{{{ns}}}Time").text
    var = member.find(f".//{{{ns}}}ParameterName").text
    value = member.find(f".//{{{ns}}}ParameterValue").text
    # Convert timestamp to datetime and value to float.
    # If there's an error, return a NaN value that will get ignored later.
    try:
        # Remove the 'Z' at the end of timestamp crudely.
        t = datetime.fromisoformat(time[:-1])
        val = float(value)
    except Exception as E:
        logging.error(f"Failed to convert timestamp of value from XML: {E}")
        return datetime.utcnow(), var, float("NaN")
    else:
        return t, var, val


def points_from_values(values: List) -> List[Dict[str, Any]]:
    """Make list of data points for AIOInflux from a list of (time, variable, value) triplets."""
    config = get_config()
    # Group the data by timestamp by putting it all in a dict. Skip missing (NaN) values.
    temp: defaultdict = defaultdict(lambda: defaultdict(dict))
    for t, var, value in values:
        if isnan(value):
            continue
        temp[t][var] = value

    # Make list of data points in the format expected by AIOInflux.
    points = []
    for t, fields in temp.items():
        points.append(
            {
                "measurement": config.influx.measurement,
                "time": t,
                "fields": fields,
                "tags": config.influx.tags,
            }
        )
    return points


async def upload_influx(points: List[Dict]):
    """Upload a list of data points to InfluxDB."""
    config = get_config()
    logging.info(f"Uploading to {config.influx.host}")
    async with aioinflux.InfluxDBClient(
        host=config.influx.host,
        username=config.influx.username,
        password=config.influx.password,
        port=config.influx.port,
        database=config.influx.database,
        ssl=True,
    ) as client:
        try:
            await client.write(points)
        except ValueError as E:
            logging.error(f"Failed to write to InfluxDB: {E}")
            return False
        except Exception as E:
            logging.error(f"Unxpected exception: {E}")
            return False
    return True


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


def get_timestamps(points):
    return [point["time"].isoformat() for point in points]


async def mainloop():
    config = get_config()
    delay = config.FMI.delay
    while True:
        logging.info("Start working...")

        # 1. Get data from FMI
        raw_data = await rawdata_from_query()

        if raw_data is None:
            logging.error(f"No data received. Waiting {delay} seconds to retry.")
            await asyncio.sleep(delay)
            continue

        logging.debug(raw_data)

        # 2. Parse XML and convert to data points for AIOInflux
        points = points_from_values(values_from_xml(xml_from_raw(raw_data)))

        if len(points) > 0:
            logging.debug(f"Timestamps: {get_timestamps(points)}")
            await upload_influx(points)
        else:
            logging.warning("No data points found in XML.")

        logging.info(f"Waiting for {delay} seconds...")
        await asyncio.sleep(delay)


def main():
    config = get_config()
    debug = config.debug_level.lower()
    logging.basicConfig(level=DEBUG_LEVELS[debug])
    logging.info(f"Debug level is '{debug}'.")

    asyncio.run(mainloop())
