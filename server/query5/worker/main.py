import os
import logging
from configparser import ConfigParser

from common.bookReceiver import BookReceiver
from common.query5Worker import Query5Worker

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
        
        config_params["category"] = os.getenv('CATEGORY', config["DEFAULT"]["CATEGORY"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    peers = config_params["peers"]
    chunk_size = config_params["chunk_size"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f"action: config | result: success | logging_level: {logging_level}")

    # Initialize server and start server loop
    books = {}
    bookHandler = BookReceiver(books, config_params["category"])
    exitcode = bookHandler.run()
    if exitcode != 0:
        return

    worker = Query5Worker(peers, chunk_size, books)
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