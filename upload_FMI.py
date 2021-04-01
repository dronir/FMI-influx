
import aiohttp
import asyncio
import requests
import toml
from collections import defaultdict
from datetime import datetime, timedelta
from math import isnan
from lxml import etree

def read_config(filename):
    """Load config from TOML file."""
    with open(filename, 'r') as f:
        return toml.loads(f.read())

def get_timestring(T):
    """ISO time string without decimals, end in Z, from datetime object."""
    return T.isoformat().split(".")[0] + "Z"

def get_paramstring(params):
    """Generate a query parameter string from dictionary."""
    kvpairs = ["&{}={}".format(*item) for item in params.items()]
    return "".join(kvpairs)


def data_from_query(QueryParams):
    """Make HTTP request, process result into data dictionary."""
    parStr = get_paramstring(QueryParams)
    QueryURL = QueryURLTemplate.format(ID=QueryID, PARAMS=parStr)
    result = requests.get(QueryURL)
    XMLTree = result_to_XML(result)
    return data_from_XML(XMLTree)
    

def result_to_XML(raw_result):
    """Turn http request result into XML tree."""
    XMLdata = bytes(raw_result.text, "utf-8")
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
