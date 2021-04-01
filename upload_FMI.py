
import aiohttp
import asyncio
import aioinflux
import toml
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from math import isnan
from sys import argv
from lxml import etree

Debug_levels = {
    "debug" : logging.DEBUG,
    "warning" : logging.WARNING,
    "info" : logging.INFO,
    "error" : logging.ERROR
}

QueryURL = "https://opendata.fmi.fi/wfs"

QueryParams = {
    "service" : "WFS",
    "storedquery_id" : "fmi::observations::weather::simple",
    "request" : "getFeature",
    "version" : "2.0.0"
}


def read_config(filename):
    """Load config from TOML file."""
    with open(filename, 'r') as f:
        return toml.loads(f.read())

def get_timestring(T):
    """ISO time string without decimals, end in Z, from datetime object."""
    return T.isoformat().split(".")[0] + "Z"


async def rawdata_from_query(session, QueryParams):
    """Make HTTP request, process result into data dictionary."""
    logging.debug(f"FMI query url: {QueryURL}")
    logging.debug(f"Query parameters: {QueryParams}")
    try:
        async with session.get(QueryURL, params=QueryParams) as response:
            logging.debug(response)
            if response.status == 200:
                return await response.text()
            else:
                return None
    except aiohttp.client_exceptions.ClientConnectorError:
        logging.error("Could not retrieve data.")
        return None



def XML_from_raw(raw_result):
    """Turn text content from HTTP request into XML tree."""
    XMLdata = bytes(raw_result, "utf-8")
    return etree.fromstring(XMLdata) 


def values_from_XML(XMLTree):
    """Return data points for AIOInflux, given XML tree."""
    # Handle namespaces properly because why not (could just wildcard them to be honest)
    ns_wfs = XMLTree.nsmap["wfs"]
    ns_bswfs = XMLTree.nsmap["BsWfs"]
    members = XMLTree.findall(f".//{{{ns_wfs}}}member")
    return [value_from_element(m, ns_bswfs) for m in members]


def value_from_element(member, ns="*"):
    """Get timestamp, variable name and value from XML element."""
    time = member.find(f".//{{{ns}}}Time").text
    var = member.find(f".//{{{ns}}}ParameterName").text
    value = member.find(f".//{{{ns}}}ParameterValue").text
    # Remove the 'Z' at the end of timestamp crudely:
    t = datetime.fromisoformat(time[:-1])
    return (t, var, float(value))


def points_from_values(influx_config, values):
    """Make a JSON-like dict structure from (time, variable name, value) pairs,
    indexed by first time stamp, then variable name.
    """
    # Group the data by timestamp by putting it all in a dict.
    temp = defaultdict(lambda: defaultdict(dict))
    for t, var, value in values:
        if isnan(value):
            continue
        temp[t][var] = value

    points = []
    for t, fields in temp.items():
        points.append({
            "measurement" : influx_config["measurement"],
            "time" : t,
            "fields" : fields,
            "tags" : {}
        })
    return points



async def upload_influx(influx_config, points):
    async with aioinflux.InfluxDBClient(
                        host=influx_config["host"],
                        port=influx_config["port"],
                        db=influx_config["database"],
                        username=influx_config["user"],
                        password=influx_config["password"],
                        ssl=True
                    ) as client:
        for point in points:
            try:
                await client.write(point)
            except ValueError as E:
                logging.error(f"Failed to write to InfluxDB: {E}")
    return True


async def mainloop(config):
    influx_config = config["influxdb"]
    FMI_config = config["FMI"]

    delay = FMI_config.get("delay", 60)
    QueryParams["place"] = FMI_config.get("location", "Kumpula")
    QueryParams["parameters"] = ",".join(FMI_config.get("variables", []))

    while True:
        now = datetime.utcnow()
        logging.info("Start working...")
        start = now - timedelta(minutes=10)
        QueryParams["starttime"] = get_timestring(start)
        QueryParams["endtime"] = get_timestring(now)

        # 1. Get data from FMI
        async with aiohttp.ClientSession() as session:
            raw_data = await rawdata_from_query(session, QueryParams)
        logging.debug(raw_data)

        # 2. Parse XML and convert to data points for AIOInflux
        XML = XML_from_raw(raw_data)
        values = values_from_XML(XML)
        points = points_from_values(influx_config, values)
        ts = [point["time"].isoformat() for point in points]
        logging.info(f"Data for times {ts} received from FMI.")

        await upload_influx(influx_config, points)
        
        logging.info(f"Data sent to Influx. Waiting for {delay} seconds...")
        await asyncio.sleep(delay)
        

if __name__ == "__main__":
    config = read_config(argv[1])

    debug = config.get("debug_level", "warning")
    logging.basicConfig(level=Debug_levels[debug])
    logging.info(f"Debug level is '{debug}'.")

    # There's actually no need to do anything with asyncio, since
    # everything happens serially right now, but possibly features
    # will be added that require concurrency...
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mainloop(config))
