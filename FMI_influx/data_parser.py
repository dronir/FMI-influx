import logging

from dataclasses import dataclass
from functools import cache
from itertools import groupby
from typing import Dict, Tuple, List, Generator, Any
from datetime import datetime
from math import isnan
from lxml import etree

from .config import get_config


@dataclass
class DataPoint:
    t: datetime
    var: str
    val: float


# A type alias
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

    The generator skips any items with a NaN value.
    Handles namespaces properly because why not (could just wildcard them to be honest).
    """
    ns_wfs = xml_tree.nsmap["wfs"]
    ns_bswfs = xml_tree.nsmap["BsWfs"]
    members = xml_tree.findall(f".//{{{ns_wfs}}}member")
    raw_points = (point_from_element(m, ns_bswfs) for m in members)
    yield from (p for p in raw_points if not isnan(p.val))


@cache
def no_data(var: str) -> DataPoint:
    """Return a dummy data point for given variable name."""
    return DataPoint(t=datetime.utcnow(), var=var, val=float("NaN"))


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
        logging.error(f"Failed to parse raw data: {time} {var} {value}")
        return no_data(var)
    except Exception as E:
        logging.error(f"Failed to convert timestamp of value from XML: {E}")
        return no_data(var)

    return DataPoint(t=t, var=var, val=val)


def group_by_time(values: DataGenerator) -> Generator[Tuple[datetime, DataGenerator], None, None]:
    """Turns data point generator into grouped (timestamp, group generator) pairs."""
    s = sorted(values, key=lambda p: p.t)
    yield from groupby(s, key=lambda p: p.t)


def payload_from_group(t: datetime, group: DataGenerator):
    """Make InfluxDB payload dict from timestamp and tuple group generator."""
    config = get_config()
    return {
        "time": t,
        "fields": {point.var: point.val for point in group},
        "measurement": config.influx.measurement,
        "tags": config.influx.tags,
    }


def parse_payload(raw_data: str) -> List[Dict[str, Any]]:
    """Take raw XML data and parse it into InfluxDB points."""
    raw_xml = xml_from_raw(raw_data)
    points = points_from_xml(raw_xml)
    groups = group_by_time(points)
    return list(payload_from_group(t, g) for t, g in groups)
