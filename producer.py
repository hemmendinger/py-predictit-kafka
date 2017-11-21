from confluent_kafka import Producer
import config
import requests
from requests.exceptions import Timeout, ConnectionError, HTTPError
import time


def run():
    server = config.HOST + ':' + config.PORT
    p = Producer({'bootstrap.servers': server})

    error_count = 0

    while True:

        try:
            req = get_api()

        except ConnectionError:
            error_count += 1
        except Timeout:
            error_count += 1
        except HTTPError:
            error_count += 1

        # If retries reaches a threshold, publish to an alerts topic

        time.sleep(config.INTERVAL)


def get_api():
    """
    :return: JSON encoded as bytes
    """
    url = 'https://www.predictit.org/api/marketdata/all'
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers=headers, timeout=(config.TIMEOUT_CONNECT, config.TIMEOUT_READ))

    return response

