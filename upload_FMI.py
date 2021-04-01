
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

logging.basicConfig(level=logging.WARNING)

QueryURLTemplate = "https://opendata.fmi.fi/wfs?{PARAMS}"

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


def get_paramstring(params):
    """Generate a query parameter string from dictionary."""
    kvpairs = ["{}={}".format(*item) for item in params.items()]
    return "&".join(kvpairs)


async def rawdata_from_query(session, QueryParams):
    """Make HTTP request, process result into data dictionary."""
    parStr = get_paramstring(QueryParams)
    QueryURL = QueryURLTemplate.format(PARAMS=parStr)
    logging.debug(QueryURL)
    try:
        async with session.get(QueryURL) as response:
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


def data_from_XML(XMLTree):
    """Return data dictionary given XML tree."""
    # Handle namespaces properly because why not (could just wildcard them to be honest)
    ns_wfs = XMLTree.nsmap["wfs"]
    ns_bswfs = XMLTree.nsmap["BsWfs"]
    members = members_from_XML(XMLTree, ns_wfs)
    values = [value_from_element(m, ns_bswfs) for m in members]
    return to_dict(values)

def members_from_XML(XMLTree, ns="*"):
    """Get individual data elements from XML tree."""
    return XMLTree.findall(f".//{{{ns}}}member")


def value_from_element(member, ns="*"):
    """Get timestamp, variable name and value from XML element."""
    time = member.find(f".//{{{ns}}}Time").text
    var = member.find(f".//{{{ns}}}ParameterName").text
    value = member.find(f".//{{{ns}}}ParameterValue").text
    # Remove the 'Z' at the end of timestamp crudely:
    t = datetime.fromisoformat(time[:-1])
    return (t, var, float(value))


def to_dict(values):
    """Make a JSON-like dict structure from (time, variable name, value) pairs,
    indexed by first time stamp, then variable name.
    """
    out = defaultdict(lambda: defaultdict(dict))
    for t, var, value in values:
        if isnan(value):
            continue
        out[t][var] = value
    # Turn the double-defaultdict into regular dicts, though probably unnecessary
    for t in out.keys():
        out[t] = dict(out[t])
    return dict(out)


def format_influx(config, data):
    points = []
    for timestamp, fields in data.items():
        point = {
            "measurement" : config["influxdb"]["measurement"],
            "time" : timestamp,
            "fields" : fields,
            "tags" : {}
        }
        points.append(point)
    return points




async def mainloop(config):
    dt = 1
    # TODO: parse config

    influx_config = config["influxdb"]

    QueryParams["place"] = config["FMI"]["location"]
    QueryParams["parameters"] = ",".join(config["FMI"]["variables"])

    while True:
        await asyncio.sleep(dt)
        
        now = datetime.utcnow()
        start = now - timedelta(minutes=10)
        QueryParams["starttime"] = get_timestring(start)
        QueryParams["endtime"] = get_timestring(now)

        # 1. Get data from FMI
        async with aiohttp.ClientSession() as session:
            raw_data = await rawdata_from_query(session, QueryParams)
        logging.debug(raw_data)
        XML = XML_from_raw(raw_data)
        data = data_from_XML(XML)

        # 2. Parse XML into something we can upload
        point = format_influx(config, data)
        
        # 3. Upload into InfluxDB  
        async with aioinflux.InfluxDBClient(
                        host=influx_config["host"],
                        port=influx_config["port"],
                        db=influx_config["database"],
                        username=influx_config["user"],
                        password=influx_config["password"],
                        ssl=True
                    ) as client:
            await client.write(point)
        logging.info("Data sent.")

        

if __name__ == "__main__":
    config = read_config(argv[1])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mainloop(config))
