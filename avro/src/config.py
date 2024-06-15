from configparser import ConfigParser


def get_config(file: str) -> ConfigParser:
    config = ConfigParser()
    config.read(file)
    return config
