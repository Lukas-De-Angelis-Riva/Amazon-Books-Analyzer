# pip install yaml
import yaml
import argparse

from config import LOGGING_LEVEL

NETWORK_NAME = "amazon-network"

def create_network(external: bool):
    net = {
        'ipam': {
            'driver': 'default',
            'config': [{'subnet': '172.25.125.0/24'}],
        }
    }
    if external:
        net['external'] = 'true'
    else:
        net['name'] = NETWORK_NAME

    return net

##################
### MIDDLEWARE ###
##################

def create_middleware():
    return {
        'build': {
            'context': './rabbitmq',
            'dockerfile': 'rabbitmq.dockerfile'
        },
        'ports': [
            '15672:15672'
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }

def create_middleware_side():
    config = {}
    config['version'] = '3.9'
    config['name'] = 'tp1-middleware'

    config['networks'] = {}
    config['networks'][NETWORK_NAME] = create_network(external = False)

    config['services'] = {}
    config['services']['rabbitmq'] = create_middleware()

    with open('docker-compose-middleware.yaml', 'w') as file:
        yaml.dump(config, file)

###################
### SERVER SIDE ###
###################

def create_clientHandler():
    return {
        'container_name': 'clientHandler',
        'image': 'client_handler:latest',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
        ],
        'volumes': [
            './server/clientHandler/config.ini:/config.ini',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }

def create_server_side():
    config = {}
    config['version'] = '3.9'
    config['name'] = 'tp1-server'

    config['networks'] = {}
    config['networks'][NETWORK_NAME] = create_network(external = True)

    config['services'] = {}
    config['services']['clientHandler'] = create_clientHandler()

    with open('docker-compose-server.yaml', 'w') as file:
        yaml.dump(config, file)

###################
### CLIENT SIDE ###
###################
def create_client():
    return {
        'container_name': 'client',
        'image': 'client:latest',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
        ],
        'volumes': [
            './client/config.ini:/config.ini',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }

def create_client_side():
    config = {}
    config['version'] = '3.9'
    config['name'] = 'tp1-client'

    config['networks'] = {}
    config['networks'][NETWORK_NAME] = create_network(external = True)

    config['services'] = {}
    config['services']['client'] = create_client()

    with open('docker-compose-client.yaml', 'w') as file:
        yaml.dump(config, file)

if __name__ == '__main__':
    create_middleware_side()
    create_server_side()
    create_client_side()
