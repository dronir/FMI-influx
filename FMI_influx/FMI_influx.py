import asyncio
import logging

from typing import AsyncGenerator, List, Dict

from .config import get_config, DEBUG_LEVELS
from .fmi_api import make_query
from .influxdb import upload_influx
from .data_parser import parse_payload


async def points_generator() -> AsyncGenerator[List[Dict], None]:
    config = get_config()
    delay = config.FMI.delay
    while True:
        if (raw_data := await make_query()) is None:
            logging.error(f"No data received. Waiting {delay} seconds to retry.")
            await asyncio.sleep(delay)
            continue

        yield parse_payload(raw_data)

        logging.debug(f"Waiting for {delay} seconds...")
        await asyncio.sleep(delay)


async def mainloop():
    async for points in points_generator():
        logging.info("Start working...")
        if len(points) > 0:
            logging.debug(f"Timestamps: {[point['time'].isoformat() for point in points]}")
            await upload_influx(points)
        else:
            logging.warning("No data points found in XML.")


def main():
    config = get_config()
    debug = config.debug_level.lower()
    logging.basicConfig(level=DEBUG_LEVELS[debug])
    logging.info(f"Debug level is '{debug}'.")

    asyncio.run(mainloop())
