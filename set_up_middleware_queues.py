import json
from config import AMOUNT_OF_QUERY1_WORKERS
from config import AMOUNT_OF_QUERY2_WORKERS
from config import AMOUNT_OF_QUERY3_WORKERS
from config import AMOUNT_OF_QUERY5_WORKERS

N_QUERIES = 5


def QUEUE_DEF(q_name: str):
    return {
        "name": q_name,
        "vhost": "/",
        "durable": True,
        "auto_delete": False,
        "arguments": {}
    }


def EXCHANGE_DEF(e_name: str, e_type: str):
    return {
        "name": e_name,
        "vhost": "/",
        "type": e_type,
        "durable": False,
        "auto_delete": False,
        "internal": False,
        "arguments": {}
    }


def BINDING_DEF(q_name, e_name, topic):
    return {
      "source": e_name,
      "vhost": "/",
      "destination": q_name,
      "destination_type": "queue",
      "routing_key": topic,
      "arguments": {}
    }


def set_up_Q1_queues():
    definitions["queues"].append(QUEUE_DEF('Q1-Sync'))
    for i in range(1, AMOUNT_OF_QUERY1_WORKERS+1):
        definitions["queues"].append(QUEUE_DEF(f'Q1-Books-{i}'))


def set_up_Q2_queues():
    definitions["queues"].append(QUEUE_DEF('Q2-Sync'))
    for i in range(1, AMOUNT_OF_QUERY2_WORKERS+1):
        definitions["queues"].append(QUEUE_DEF(f'Q2-Books-{i}'))


def set_up_Q3_queues():
    definitions["queues"].append(QUEUE_DEF('Q3-Sync'))
    for i in range(1, AMOUNT_OF_QUERY3_WORKERS+1):
        definitions["queues"].append(QUEUE_DEF(f'Q3-Books-{i}'))
        definitions["queues"].append(QUEUE_DEF(f'Q3-Reviews-{i}'))


def set_up_Q5_queues():
    definitions["queues"].append(QUEUE_DEF('Q5-Sync'))
    for i in range(1, AMOUNT_OF_QUERY5_WORKERS+1):
        definitions["queues"].append(QUEUE_DEF(f'Q5-Books-{i}'))
        definitions["queues"].append(QUEUE_DEF(f'Q5-Reviews-{i}'))


def set_up_RH_queues():
    definitions["queues"].append(QUEUE_DEF('RH-Results'))
    for i in range(1, N_QUERIES + 1):
        definitions["bindings"].append(BINDING_DEF('RH-Results', 'results', f'Q{i}'))


def set_up_exchanges():
    definitions["exchanges"].append(EXCHANGE_DEF('results', 'direct'))


definitions = {}


if __name__ == '__main__':
    definitions["exchanges"] = []
    definitions["queues"] = []
    definitions["bindings"] = []

    set_up_exchanges()

    set_up_Q1_queues()
    set_up_Q2_queues()
    set_up_Q3_queues()
    set_up_Q5_queues()
    set_up_RH_queues()

    with open("rabbitmq/definitions.json", "w") as def_file:
        json.dump(definitions, fp=def_file, indent=4)
