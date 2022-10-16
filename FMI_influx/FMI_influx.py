import asyncio
import logging

from itertools import groupby
from typing import Dict, Tuple, List, Generator, AsyncGenerator
from datetime import datetime
from math import isnan
from lxml import etree

from .config import get_config, DEBUG_LEVELS
from .fmi_api import make_query
from .influxdb import upload_influx


# Some type aliases:
DataPoint = Tuple[datetime, str, float]
DataGenerator = Generator[DataPoint, None, None]


def xml_from_raw(raw_result: str) -> etree.Element:
    """Turn plain text content from HTTP request into XML tree."""
    xml_data = bytes(raw_result, "utf-8")
    try:
        return etree.fromstring(xml_data)
    except Exception as E:
        logging.error(f"Error while parsing XML data:\n {E}")
        return etree.Element("root")  # Return an empty XML tree.


def values_from_xml(xml_tree: etree.Element) -> DataGenerator:
    """Return a generator of (time, variable, value) tuples, given XML tree.

    The generator skips any items with a NaN value."""
    # Handle namespaces properly because why not (could just wildcard them to be honest)
    ns_wfs = xml_tree.nsmap["wfs"]
    ns_bswfs = xml_tree.nsmap["BsWfs"]
    members = xml_tree.findall(f".//{{{ns_wfs}}}member")
    raw_values = (value_from_element(m, ns_bswfs) for m in members)
    yield from (p for p in raw_values if not isnan(p[2]))


def value_from_element(member: etree.Element, ns: str = "*") -> DataPoint:
    """Get one (time, variable, value) tuple from the XML element containing it."""
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


def time_key(p: DataPoint) -> datetime:
    return p[0]


def group_by_time(values: DataGenerator) -> Generator[Tuple[datetime, DataGenerator], None, None]:
    """Generator that returns (timestamp, group generator) pairs."""
    s = sorted(values, key=time_key)
    yield from groupby(s, key=time_key)


def fields_from_group(group: DataGenerator) -> Dict[str, float]:
    """Get InfluxDB fields from tuple group generator."""
    return {k: v for t, k, v in group}


def points_from_group(t: datetime, group: DataGenerator):
    """Make InfluxDB payload dict from timestamp and tuple group generator."""
    config = get_config()
    return {
        "time": t,
        "fields": fields_from_group(group),
        "measurement": config.influx.measurement,
        "tags": config.influx.tags,
    }


def get_timestamps(points: List[Dict]) -> List[str]:
    return [point["time"].isoformat() for point in points]


async def points_generator() -> AsyncGenerator[List[Dict], None]:
    config = get_config()
    delay = config.FMI.delay
    while True:
        if (raw_data := await make_query()) is None:
            logging.error(f"No data received. Waiting {delay} seconds to retry.")
            await asyncio.sleep(delay)
            continue

        raw_xml = xml_from_raw(raw_data)
        values = values_from_xml(raw_xml)
        grouped = group_by_time(values)
        points = (points_from_group(t, g) for t, g in grouped)
        yield list(points)

        logging.debug(f"Waiting for {delay} seconds...")
        await asyncio.sleep(delay)


async def mainloop():
    async for points in points_generator():
        logging.info("Start working...")
        if len(points) > 0:
            logging.debug(f"Timestamps: {get_timestamps(points)}")
            await upload_influx(points)
        else:
            logging.warning("No data points found in XML.")


def main():
    config = get_config()
    debug = config.debug_level.lower()
    logging.basicConfig(level=DEBUG_LEVELS[debug])
    logging.info(f"Debug level is '{debug}'.")

    asyncio.run(mainloop())
