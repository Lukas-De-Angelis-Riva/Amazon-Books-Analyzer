# pip install yaml
import yaml

from config import LOGGING_LEVEL
from config import AMOUNT_OF_QUERY1_WORKERS
from config import AMOUNT_OF_QUERY2_WORKERS
from config import AMOUNT_OF_QUERY3_WORKERS
from config import AMOUNT_OF_QUERY5_WORKERS

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

##############
# MIDDLEWARE #
##############


def create_middleware():
    return {
        'build': {
            'context': './rabbitmq',
            'dockerfile': 'rabbitmq.dockerfile'
        },
        'ports': [
            '15672:15672'
        ],
        'healthcheck': {
            'test': 'rabbitmq-diagnostics check_port_connectivity',
            'interval': '10s',
            'timeout': '10s',
            'retries': '5',
        },
        'volumes': [
            './rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro',
            './rabbitmq/definitions.json:/etc/rabbitmq/definitions.json:ro'
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }

###############
# SERVER SIDE #
###############


def create_query1Worker(i):
    return {
        'container_name': f'query1Worker{i}',
        'image': 'query1_worker:latest',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
            'PEERS='+str(AMOUNT_OF_QUERY1_WORKERS),
            'PEER_ID='+str(i),
        ],
        'volumes': [
            './server/query1/worker/config.ini:/config.ini',
        ],
        'depends_on': [
            'query1Synchronizer',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }


def create_query1Synchronizer():
    return {
        'container_name': 'query1Synchronizer',
        'image': 'query1_synchronizer:latest',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
            'N_WORKERS='+str(AMOUNT_OF_QUERY1_WORKERS),
        ],
        'volumes': [
            './server/query1/synchronizer/config.ini:/config.ini',
        ],
        'depends_on': [
            'resultHandler',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }


def create_query2Worker(i):
    return {
        'container_name': f'query2Worker{i}',
        'image': 'query2_worker:latest',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
            'PEERS='+str(AMOUNT_OF_QUERY2_WORKERS),
            'PEER_ID='+str(i),
        ],
        'volumes': [
            './server/query2/worker/config.ini:/config.ini',
        ],
        'depends_on': [
            'query2Synchronizer',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }


def create_query2Synchronizer():
    return {
        'container_name': 'query2Synchronizer',
        'image': 'query2_synchronizer:latest',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
            'N_WORKERS='+str(AMOUNT_OF_QUERY2_WORKERS),
        ],
        'volumes': [
            './server/query2/synchronizer/config.ini:/config.ini',
        ],
        'depends_on': [
            'resultHandler',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }


def create_query3Worker(i):
    return {
        'container_name': f'query3Worker{i}',
        'image': 'query3_worker:latest',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
            'PEERS='+str(AMOUNT_OF_QUERY3_WORKERS),
            'PEER_ID='+str(i),
        ],
        'volumes': [
            './server/query3/worker/config.ini:/config.ini',
        ],
        'depends_on': [
            'query3Synchronizer',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }


def create_query3Synchronizer():
    return {
        'container_name': 'query3Synchronizer',
        'image': 'query3_synchronizer:latest',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
            'N_WORKERS='+str(AMOUNT_OF_QUERY3_WORKERS),
        ],
        'volumes': [
            './server/query3/synchronizer/config.ini:/config.ini',
        ],
        'depends_on': [
            'resultHandler',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }


def create_query5Worker(i):
    return {
        'container_name': f'query5Worker{i}',
        'image': 'query5_worker:latest',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
            'PEERS='+str(AMOUNT_OF_QUERY5_WORKERS),
            'PEER_ID='+str(i),
        ],
        'volumes': [
            './server/query5/worker/config.ini:/config.ini',
        ],
        'depends_on': [
            'query5Synchronizer',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }


def create_query5Synchronizer():
    return {
        'container_name': 'query5Synchronizer',
        'image': 'query5_synchronizer:latest',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
            'N_WORKERS='+str(AMOUNT_OF_QUERY5_WORKERS),
        ],
        'volumes': [
            './server/query5/synchronizer/config.ini:/config.ini',
        ],
        'depends_on': [
            'resultHandler',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }


def create_resultHandler():
    return {
        'container_name': 'resultHandler',
        'image': 'result_handler:latest',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
        ],
        'volumes': [
            './server/resultHandler/config.ini:/config.ini',
        ],
        'depends_on': {
            'rabbitmq': {
                'condition': 'service_healthy'
            }
        },
        'networks': [
            NETWORK_NAME,
        ],
    }


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
        'depends_on':
            [f'query1Worker{i+1}' for i in range(AMOUNT_OF_QUERY1_WORKERS)] +
            [f'query2Worker{i+1}' for i in range(AMOUNT_OF_QUERY2_WORKERS)] +
            [f'query3Worker{i+1}' for i in range(AMOUNT_OF_QUERY3_WORKERS)] +
            [f'query5Worker{i+1}' for i in range(AMOUNT_OF_QUERY5_WORKERS)],
        'networks': [
            NETWORK_NAME,
        ],
    }


def create_server_side():
    config = {}
    config['version'] = '3.9'
    config['name'] = 'tp2-server'

    # NETWORK
    config['networks'] = {}
    config['networks'][NETWORK_NAME] = create_network(external=False)

    # MIDDLEWARE
    config['services'] = {}
    config['services']['rabbitmq'] = create_middleware()

    # CLIENT HANDLER
    config['services']['clientHandler'] = create_clientHandler()

    # QUERY 1
    for i in range(AMOUNT_OF_QUERY1_WORKERS):
        config['services'][f'query1Worker{i+1}'] = create_query1Worker(i+1)
    config['services']['query1Synchronizer'] = create_query1Synchronizer()

    # QUERY 2
    for i in range(AMOUNT_OF_QUERY2_WORKERS):
        config['services'][f'query2Worker{i+1}'] = create_query2Worker(i+1)
    config['services']['query2Synchronizer'] = create_query2Synchronizer()

    # QUERY 3 & 4
    for i in range(AMOUNT_OF_QUERY3_WORKERS):
        config['services'][f'query3Worker{i+1}'] = create_query3Worker(i+1)
    config['services']['query3Synchronizer'] = create_query3Synchronizer()

    # QUERY 5
    for i in range(AMOUNT_OF_QUERY5_WORKERS):
        config['services'][f'query5Worker{i+1}'] = create_query5Worker(i+1)
    config['services']['query5Synchronizer'] = create_query5Synchronizer()

    config['services']['resultHandler'] = create_resultHandler()

    with open('docker-compose-server.yaml', 'w') as file:
        yaml.dump(config, file)


###############
# CLIENT SIDE #
###############
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
            './client/data:/data',
        ],
        'tty': True,
        'networks': [
            NETWORK_NAME,
        ],
    }


def create_client_side():
    config = {}
    config['version'] = '3.9'
    config['name'] = 'tp1-client'

    config['networks'] = {}
    config['networks'][NETWORK_NAME] = create_network(external=True)

    config['services'] = {}
    config['services']['client'] = create_client()

    with open('docker-compose-client.yaml', 'w') as file:
        yaml.dump(config, file)


if __name__ == '__main__':
    create_server_side()
    create_client_side()
