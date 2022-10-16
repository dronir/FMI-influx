import asyncio
import logging

from dataclasses import dataclass
from itertools import groupby
from typing import Dict, Tuple, List, Generator, AsyncGenerator
from datetime import datetime
from math import isnan
from lxml import etree

from .config import get_config, DEBUG_LEVELS
from .fmi_api import make_query
from .influxdb import upload_influx


# Some type aliases:
@dataclass
class DataPoint:
    t: datetime
    var: str
    val: float


DataGenerator = Generator[DataPoint, None, None]


def xml_from_raw(raw_result: str) -> etree.Element:
    """Turn plain text content from HTTP request into XML tree."""
    xml_data = bytes(raw_result, "utf-8")
    try:
        return etree.fromstring(xml_data)
    except Exception as E:
        logging.error(f"Error while parsing XML data:\n {E}")
        return etree.Element("root")  # Return an empty XML tree.


def points_from_xml(xml_tree: etree.Element) -> DataGenerator:
    """Return a generator of (time, variable, value) tuples, given XML tree.

    The generator skips any items with a NaN value."""
    # Handle namespaces properly because why not (could just wildcard them to be honest)
    ns_wfs = xml_tree.nsmap["wfs"]
    ns_bswfs = xml_tree.nsmap["BsWfs"]
    members = xml_tree.findall(f".//{{{ns_wfs}}}member")
    raw_points = (point_from_element(m, ns_bswfs) for m in members)
    yield from (p for p in raw_points if not isnan(p.val))


def point_from_element(member: etree.Element, ns: str = "*") -> DataPoint:
    """Get one (time, variable, value) tuple from the XML element containing it."""
    time = member.find(f".//{{{ns}}}Time").text
    var = member.find(f".//{{{ns}}}ParameterName").text
    value = member.find(f".//{{{ns}}}ParameterValue").text
    # Convert timestamp to datetime and value to float.
    # If there's an error, return a NaN value that will get ignored later.
    try:
        t = datetime.fromisoformat(time[:-1])  # Remove the 'Z' at the end of timestamp
        val = float(value)
    except ValueError:
        logging.error(f"Failed to parse value: {time} {var} {value}")
        return DataPoint(t=datetime.utcnow(), var=var, val=float("NaN"))
    except Exception as E:
        logging.error(f"Failed to convert timestamp of value from XML: {E}")
        return DataPoint(t=datetime.utcnow(), var=var, val=float("NaN"))
    else:
        return DataPoint(t=t, var=var, val=val)


def group_by_time(values: DataGenerator) -> Generator[Tuple[datetime, DataGenerator], None, None]:
    """Turns data point generator into grouped (timestamp, group generator) pairs."""
    s = sorted(values, key=lambda p: p.t)
    yield from groupby(s, key=lambda p: p.t)


def fields_from_group(group: DataGenerator) -> Dict[str, float]:
    """Get InfluxDB fields from tuple group generator."""
    return {point.var: point.val for point in group}


def payload_from_group(t: datetime, group: DataGenerator):
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
        points = points_from_xml(raw_xml)
        groups = group_by_time(points)
        yield list(payload_from_group(t, g) for t, g in groups)

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
