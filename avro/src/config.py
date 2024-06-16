from configparser import ConfigParser


# reads the config from the config.ini
def get_config(file: str) -> ConfigParser:
    config = ConfigParser()
    config.read(file)
    return config
