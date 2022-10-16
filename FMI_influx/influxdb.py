import logging
import aioinflux

from typing import List, Dict, Any
from .config import get_config


async def upload_influx(points: List[Dict[str, Any]]):
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
