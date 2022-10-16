import logging

from typing import List, Dict
from functools import cache
from pydantic import BaseSettings, Field


DEBUG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
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
        env_file = ".env"
        env_prefix = "UPLOADER_"


@cache
def get_config():
    """Reads config from environmental variables or .env file."""
    return Config()
