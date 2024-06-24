from configparser import ConfigParser
from common.clientHandler import ClientHandler
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
        config_params["max_users"] = int(os.getenv('MAX_USERS', config["DEFAULT"]["MAX_USERS"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["n_workers_q1"] = int(os.getenv('N_WORKERS_Q1', config["DEFAULT"]["N_WORKERS_Q1"]))
        config_params["n_workers_q2"] = int(os.getenv('N_WORKERS_Q2', config["DEFAULT"]["N_WORKERS_Q2"]))
        config_params["n_workers_q3"] = int(os.getenv('N_WORKERS_Q3', config["DEFAULT"]["N_WORKERS_Q3"]))
        config_params["n_workers_q5"] = int(os.getenv('N_WORKERS_Q5', config["DEFAULT"]["N_WORKERS_Q5"]))

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    port = config_params["port"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f"action: config | result: success | port: {port} |"
                  f"logging_level: {logging_level}")

    # Initialize server and start server loop
    clientHandler = ClientHandler(config_params)
    clientHandler.run()


if __name__ == "__main__":
    main()
