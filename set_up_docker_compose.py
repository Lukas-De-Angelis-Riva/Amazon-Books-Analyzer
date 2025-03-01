# pip install yaml
import yaml

from config import LOGGING_LEVEL
from config import AMOUNT_OF_QUERY1_WORKERS
from config import AMOUNT_OF_QUERY2_WORKERS
from config import AMOUNT_OF_QUERY3_WORKERS
from config import AMOUNT_OF_QUERY5_WORKERS
from config import AMOUNT_OF_DOCTOR

NETWORK_NAME = "amazon-network"

HEARTBEAT_PORT = '12349'


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


def list_of_nodes():
    return [f'doctor{i+1}' for i in range(AMOUNT_OF_DOCTOR)] + \
            [f'query1Worker{i}' for i in range(1, AMOUNT_OF_QUERY1_WORKERS+1)] + \
            [f'query2Worker{i}' for i in range(1, AMOUNT_OF_QUERY2_WORKERS+1)] + \
            [f'query3Worker{i}' for i in range(1, AMOUNT_OF_QUERY3_WORKERS+1)] + \
            [f'query5Worker{i}' for i in range(1, AMOUNT_OF_QUERY5_WORKERS+1)] + \
            ['query1Synchronizer'] + \
            ['query2Synchronizer'] + \
            ['query3Synchronizer'] + \
            ['query5Synchronizer'] + \
            ['resultHandler'] + \
            ['clientHandler']


def create_doctor(i):
    return {
        'container_name': f'doctor{i}',
        'image': 'doctor:latest',
        'privileged': 'true',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'LOGGING_LEVEL={LOGGING_LEVEL}',
            'PEERS='+str(AMOUNT_OF_DOCTOR),
            'PEER_ID='+str(i),
            f'NODES={list_of_nodes()}',
        ],
        'volumes': [
            './server/doctor/config.ini:/config.ini',
            '/var/run/docker.sock:/var/run/docker.sock',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }

def create_monkey():
    return {
        'container_name': f'chaosMonkey',
        'image': 'chaos_monkey:latest',
        'privileged': 'true',
        'entrypoint': 'python3 /main.py',
        'environment': [
            'PYTHONUNBUFFERED=1',
            f'NODES={list_of_nodes()}',
        ],
        'volumes': [
            './chaosMonkey/config.ini:/config.ini',
            '/var/run/docker.sock:/var/run/docker.sock',
        ],
        'networks': [
            NETWORK_NAME,
        ],
    }

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
            f'HEARTBEAT_IP=query1Worker{i}',
            f'HEARTBEAT_PORT={HEARTBEAT_PORT}',
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
            'HEARTBEAT_IP=query1Synchronizer',
            f'HEARTBEAT_PORT={HEARTBEAT_PORT}',
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
            f'HEARTBEAT_IP=query2Worker{i}',
            f'HEARTBEAT_PORT={HEARTBEAT_PORT}',
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
            'HEARTBEAT_IP=query2Synchronizer',
            f'HEARTBEAT_PORT={HEARTBEAT_PORT}',
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
            f'HEARTBEAT_IP=query3Worker{i}',
            f'HEARTBEAT_PORT={HEARTBEAT_PORT}',
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
            'HEARTBEAT_IP=query3Synchronizer',
            f'HEARTBEAT_PORT={HEARTBEAT_PORT}',
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
            f'HEARTBEAT_IP=query5Worker{i}',
            f'HEARTBEAT_PORT={HEARTBEAT_PORT}',
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
            'HEARTBEAT_IP=query5Synchronizer',
            f'HEARTBEAT_PORT={HEARTBEAT_PORT}',
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
            'HEARTBEAT_IP=resultHandler',
            f'HEARTBEAT_PORT={HEARTBEAT_PORT}',
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
            f'N_WORKERS_Q1={AMOUNT_OF_QUERY1_WORKERS}',
            f'N_WORKERS_Q2={AMOUNT_OF_QUERY2_WORKERS}',
            f'N_WORKERS_Q3={AMOUNT_OF_QUERY3_WORKERS}',
            f'N_WORKERS_Q5={AMOUNT_OF_QUERY5_WORKERS}',
            'HEARTBEAT_IP=clientHandler',
            f'HEARTBEAT_PORT={HEARTBEAT_PORT}',
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
    config['name'] = 'tp2-server'

    # NETWORK
    config['networks'] = {}
    config['networks'][NETWORK_NAME] = create_network(external=False)

    config['services'] = {}

    # MIDDLEWARE
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

    # DOCTOR
    for i in range(AMOUNT_OF_DOCTOR):
        config['services'][f'doctor{i+1}'] = create_doctor(i+1)

    config['services']['chaosMonkey'] = create_monkey()

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
