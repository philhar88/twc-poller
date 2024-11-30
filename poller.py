#!/usr/bin/env python3

# Standard library imports
import argparse
import logging
import os
import random
import signal
import sys
import threading
import time

# Third-party imports
import redis
import requests
import yaml

# URL parsing
from urllib.parse import urlparse, urlunparse, urljoin

# Default configuration file path
DEFAULT_CONFIG_PATH = '/etc/poller/config.yml'

# Global variables
config_lock = threading.Lock()
threads = []
stop_event = threading.Event()
REDIS_CLIENT = None
config = {}
redis_config = {}
global_config = {}
logger = logging.getLogger(__name__)

def load_config(config_file):
    """
    Load and parse the YAML configuration file.
    """
    logger.debug("Loading configuration from file: %s", config_file)
    if not config_file:
        logger.critical("No configuration file specified. Please provide a config file path via CLI argument or environment variable.")
        sys.exit(1)
        
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
        logger.debug("Configuration loaded successfully.")
        return cfg
    except FileNotFoundError:
        logger.critical("Configuration file not found: %s", config_file)
        sys.exit(1)
    except yaml.YAMLError as e:
        logger.critical("Error loading configuration file: %s", e)
        sys.exit(1)

def apply_defaults(address):
    """
    Ensure the address has a protocol and port.
    """
    logger.debug("Applying defaults to address: %s", address)
    parsed = urlparse(address if '://' in address else f'http://{address}')
    scheme = parsed.scheme if parsed.scheme else 'http'
    netloc = parsed.netloc if parsed.netloc else parsed.path
    path = parsed.path if parsed.netloc else ''
    full_address = urlunparse((scheme, netloc, path, '', '', ''))
    logger.debug("Full address after applying defaults: %s", full_address)
    return full_address

def poll_endpoint(address, endpoint_config, session, stop_event, poll_interval, random_offset_max, api_prefix, redis_stream_key, connector):
    """
    Poll a single endpoint at the specified address with exponential backoff and jitter.
    """
    endpoint_name = endpoint_config['endpoint'] if isinstance(endpoint_config, dict) else endpoint_config
    endpoint_interval = endpoint_config.get('interval', poll_interval) if isinstance(endpoint_config, dict) else poll_interval
    
    url = urljoin(address, api_prefix + endpoint_name)
    logger.debug("Polling endpoint: %s with interval: %s seconds", url, endpoint_interval)
    backoff_factor = 0.5
    max_backoff = 60  # Max backoff interval in seconds
    retry_attempts = 0

    # Debug: Check connection reuse
    session.hooks['response'] = lambda r, *args, **kwargs: logger.debug(
        f"Connection reused for {r.url}: {r.raw._connection.sock is not None}"
    )

    while not stop_event.is_set():
        start_time = time.time()
        try:
            if stop_event.is_set():
                break

            logger.debug("Sending request to: %s", url)
            response = session.get(
                url,
                timeout=(3.05, 27),  # (connect timeout, read timeout) in seconds
            )
            response.raise_for_status()
            response_data = response.text
            logger.debug("Received response: %s", response_data)
            
            # Add to Redis stream
            REDIS_CLIENT.xadd(redis_stream_key, {
                'address': address, 
                'endpoint': endpoint_name,
                'serial_number': connector.get('serial_number', 'UNKNOWN'),
                'part_number': connector.get('part_number', 'UNKNOWN'),
                'data': response_data,
            })
            logger.debug("Data added to Redis stream %s: %s", redis_stream_key, response_data)

            retry_attempts = 0  # Reset backoff on success
        except Exception as e:
            logger.error("Error polling %s: %s", url, e)
            retry_attempts += 1
            sleep_time = min(max_backoff, (2 ** retry_attempts) * backoff_factor)
            sleep_time = sleep_time * (0.5 + random.random() / 2)  # Add jitter
            logger.debug("Retrying %s in %.2f seconds", url, sleep_time)
            time.sleep(sleep_time)
            continue
        else:
            elapsed_time = time.time() - start_time
            sleep_time = endpoint_interval - elapsed_time + random.uniform(0, random_offset_max)
            if sleep_time > 0:
                stop_event.wait(sleep_time)

    logger.debug("Polling stopped for endpoint %s", endpoint_name)

def setup_logging():
    """
    Set up logging with debug hooks for requests and urllib3.
    """
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    # Enable HTTP client debugging
    import http.client as http_client
    http_client.HTTPConnection.debuglevel = 1

    # Enable logging for requests and urllib3
    logging.getLogger("requests").setLevel(logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.DEBUG)

    logger.debug("Logging setup with full debugging enabled.")

def setup_redis():
    """
    Set up the Redis client based on configuration.
    """
    logger.debug("Setting up Redis client.")
    global REDIS_CLIENT
    redis_host = redis_config.get('host', 'localhost')
    redis_port = redis_config.get('port', 6379)
    redis_db = redis_config.get('db', 0)
    redis_password = redis_config.get('password', None)

    REDIS_CLIENT = redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
        socket_timeout=5
    )
    logger.debug("Redis client setup complete.")

def main():
    """
    Main function to start the poller.
    """
    setup_logging()
    logger.info("Starting poller...")
    
    # Example configuration
    address = "http://vorignet.com:5222"
    endpoint_config = {'endpoint': '/api/1/vitals'}
    session = requests.Session()

    # Simulate poller
    stop_event = threading.Event()
    try:
        poll_endpoint(
            address,
            endpoint_config,
            session,
            stop_event,
            poll_interval=10,
            random_offset_max=0.1,
            api_prefix="",
            redis_stream_key="example_key",
            connector={"serial_number": "12345", "part_number": "67890"}
        )
    except KeyboardInterrupt:
        stop_event.set()
        logger.info("Poller stopped.")

if __name__ == '__main__':
    main()
