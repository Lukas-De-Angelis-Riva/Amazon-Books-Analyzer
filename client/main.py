from configparser import ConfigParser
from common.client import Client
import logging
import os

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
        config_params["port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["ip"] = os.getenv('SERVER_IP', config["DEFAULT"]["SERVER_IP"])

        config_params["book_file_path"] = os.getenv('BOOK_FILE_PATH', config["DEFAULT"]["BOOK_FILE_PATH"])
        config_params["chunk_size_book"] = int(os.getenv('CHUNK_SIZE_BOOK', config["DEFAULT"]["CHUNK_SIZE_BOOK"]))

        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    port = config_params["port"]
    ip = config_params["ip"]
    book_file_path = config_params["book_file_path"]    
    chunk_size_book = config_params["chunk_size_book"]    

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f'''action: config | result: success
                  | server-ip: {ip}
                  | server-port: {port}
                  | logging_level: {logging_level}''')

    # Initialize server and start server loop
    client = Client(ip, port, book_file_path,chunk_size_book)
    client.run()

if __name__ == "__main__":
    main()