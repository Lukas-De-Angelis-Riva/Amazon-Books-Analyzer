from configparser import ConfigParser
from common.query1Worker import Query1Worker
from common.matches import matching_function
import logging
import os


def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file.
    If at least one of the config parameters is not found a KeyError exception
    is thrown. If a parameter could not be parsed, a ValueError is thrown.
    If parsing succeeded, the function returns a ConfigParser object
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["chunk_size"] = int(os.getenv('CHUNK_SIZE', config["DEFAULT"]["CHUNK_SIZE"]))
        config_params["peers"] = int(os.environ['PEERS'])
        config_params["peer_id"] = int(os.environ['PEER_ID'])

        config_params["published_date_min"] = int(os.getenv('PUBLISHED_DATE_MIN', config["DEFAULT"]["PUBLISHED_DATE_MIN"]))
        config_params["published_date_max"] = int(os.getenv('PUBLISHED_DATE_MAX', config["DEFAULT"]["PUBLISHED_DATE_MAX"]))
        config_params["category"] = os.getenv('CATEGORY', config["DEFAULT"]["CATEGORY"])
        config_params["title"] = os.getenv('TITLE', config["DEFAULT"]["TITLE"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    chunk_size = config_params["chunk_size"]
    peers = config_params['peers']
    peer_id = config_params['peer_id']

    published_date_min = config_params["published_date_min"]
    published_date_max = config_params["published_date_max"]
    category = config_params["category"]
    title = config_params["title"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f"action: config | result: success | logging_level: {logging_level}")

    # Initialize server and start server loop
    matches = matching_function(published_date_min, published_date_max, category, title)
    worker = Query1Worker(peer_id, peers, chunk_size, matches)
    exitcode = worker.run()
    return exitcode


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()
