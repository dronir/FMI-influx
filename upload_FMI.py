import aiohttp
import asyncio
import aioinflux
import toml
import logging
from typing import Dict, Any, Tuple, List
from collections import defaultdict
from datetime import datetime, timedelta
from math import isnan
from sys import argv, exit
from lxml import etree

DEBUG_LEVELS = {
    "debug": logging.DEBUG,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "error": logging.ERROR,
}

QUERY_URL = "https://opendata.fmi.fi/wfs"

DEFAULT_QUERY_PARAMS = {
    "service": "WFS",
    "storedquery_id": "fmi::observations::weather::simple",
    "request": "getFeature",
    "version": "2.0.0",
}


def read_config(filename) -> Dict[str, Any]:
    """Load config from TOML file."""
    with open(filename, "r") as f:
        return toml.loads(f.read())


def get_timestring(t: datetime) -> str:
    """ISO time string without decimals, end in Z, from datetime object."""
    return t.isoformat().split(".")[0] + "Z"


async def rawdata_from_query(session, query_params: Dict):
    """Make HTTP request, return text contents."""
    logging.debug(f"FMI query url: {QUERY_URL}")
    logging.debug(f"Query parameters: {query_params}")
    try:
        async with session.get(QUERY_URL, params=query_params) as response:
            logging.debug(response)
            if response.status == 200:
                return await response.text()
            else:
                logging.error(f"HTTP error {response.status} when fetching data.")
                return None
    except aiohttp.client_exceptions.ClientConnectorError as E:
        logging.error(f"Error while retrieving data: {E}")
        return None


def xml_from_raw(raw_result: str) -> None:
    """Turn text content from HTTP request into XML tree."""
    xml_data = bytes(raw_result, "utf-8")
    try:
        return etree.fromstring(xml_data)
    except Exception as E:
        logging.error(f"Exception while parsing XML data:\n {E}")
        return etree.Element("root")  # Return an empty XML tree.


def values_from_xml(xml_tree) -> List:
    """Return all (time, variable, value) triplets, given XML tree."""
    # Handle namespaces properly because why not (could just wildcard them to be honest)
    ns_wfs = xml_tree.nsmap["wfs"]
    ns_bswfs = xml_tree.nsmap["BsWfs"]
    members = xml_tree.findall(f".//{{{ns_wfs}}}member")
    return [value_from_element(m, ns_bswfs) for m in members]


def value_from_element(member, ns="*") -> Tuple:
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


def points_from_values(influx_config: Dict[str,Any], values: List):
    """Make list of data points for AIOInflux from a list of (time, variable, value) triplets."""

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
                "measurement": influx_config["measurement"],
                "time": t,
                "fields": fields,
                "tags": influx_config.get("tags", {}),
            }
        )
    return points


async def upload_influx(influx_config, points):
    """Upload a list of data points to InfluxDB."""
    async with aioinflux.InfluxDBClient(
        **influx_config["connection"], ssl=True
    ) as client:
        for point in points:
            try:
                await client.write(point)
            except ValueError as E:
                logging.error(f"Failed to write to InfluxDB: {E}")
    return True


def check_config(config):
    return "influxdb" in config and check_config_influx(config["influxdb"])


def check_config_influx(config):
    return (
        "connection" in config
        and "host" in config["connection"]
        and "port" in config["connection"]
        and "username" in config["connection"]
        and "password" in config["connection"]
        and "db" in config["connection"]
    )


async def mainloop(config):
    influx_config = config["influxdb"]
    fmi_config = config["FMI"]

    delay = fmi_config.get("delay", 60)
    history = fmi_config.get("history", 10)

    query_params = DEFAULT_QUERY_PARAMS.copy()
    query_params["place"] = fmi_config.get("location", "Kumpula")
    query_params["parameters"] = ",".join(fmi_config.get("variables", []))

    while True:
        now = datetime.utcnow()
        logging.info("Start working...")
        start = now - timedelta(minutes=history)
        query_params["starttime"] = get_timestring(start)
        query_params["endtime"] = get_timestring(now)

        # 1. Get data from FMI
        async with aiohttp.ClientSession() as session:
            raw_data = await rawdata_from_query(session, query_params)
        if raw_data == None:
            logging.error(f"No data received. Waiting {delay} seconds to retry.")
            await asyncio.sleep(delay)
            continue

        logging.debug(raw_data)

        # 2. Parse XML and convert to data points for AIOInflux
        xml_data = xml_from_raw(raw_data)
        values = values_from_xml(xml_data)
        points = points_from_values(influx_config, values)

        if len(points) > 0:
            ts = [point["time"].isoformat() for point in points]
            logging.info(f"Data for timestamps {ts} received from FMI.")
            await upload_influx(influx_config, points)
        else:
            logging.warning(f"No data points found in XML.")

        logging.info(f"Waiting for {delay} seconds...")
        await asyncio.sleep(delay)


if __name__ == "__main__":
    config = read_config(argv[1])
    if not check_config(config):
        logging.error("Config file is not ok.")
        exit()

    # Set logging levels.
    debug = config.get("debug_level", "warning")
    logging.basicConfig(level=DEBUG_LEVELS[debug])
    logging.info(f"Debug level is '{debug}'.")

    # Run main task asynchronously.
    # There's actually no need to do anything with asyncio, since
    # everything happens serially right now, but maybe features
    # will be added that require concurrency...
    # I mostly wanted to check out AIOInflux.
    asyncio.run(mainloop(config))
