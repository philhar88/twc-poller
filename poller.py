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
    # Extract endpoint name and interval from config
    endpoint_name = endpoint_config['endpoint'] if isinstance(endpoint_config, dict) else endpoint_config
    endpoint_interval = endpoint_config.get('interval', poll_interval) if isinstance(endpoint_config, dict) else poll_interval
    
    url = urljoin(address, api_prefix + endpoint_name)
    logger.debug("Polling endpoint: %s with interval: %s seconds", url, endpoint_interval)
    backoff_factor = 0.5
    max_backoff = 60  # Max backoff interval in seconds
    retry_attempts = 0

    # Configure the session with better timeout and pooling settings
    session.headers.update({'Connection': 'keep-alive'})
    
    # Create adapter with debug logging for connection reuse
    class DebugHTTPAdapter(requests.adapters.HTTPAdapter):
        def get_connection(self, url, proxies=None):
            conn = super().get_connection(url, proxies)
            logger.debug("Connection reuse for %s: %s", url, conn.is_reused())
            return conn
    
    adapter = DebugHTTPAdapter(
        pool_connections=10,    # Number of connection pools to cache
        pool_maxsize=10,       # Number of connections to save in the pool
        max_retries=0,         # We handle retries ourselves
        pool_block=False       # Don't block when pool is full
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    while not stop_event.is_set():
        start_time = time.time()
        try:
            # Check stop_event before making request
            if stop_event.is_set():
                break

            logger.debug("Sending request to: %s", url)
            # Split timeouts into connect and read
            response = session.get(
                url,
                timeout=(3.05, 27),  # (connect timeout, read timeout) in seconds
            )
            response.raise_for_status()
            response_data = response.text
            logger.debug("Received response: %s", response_data)
            
            # Add to Redis stream with retry logic
            redis_retry_attempts = 0
            redis_backoff_factor = 0.5
            redis_max_backoff = 5  # Aggressive retry for Redis, max 5 seconds
            while True:
                try:
                    REDIS_CLIENT.xadd(redis_stream_key, {
                        'address': address, 
                        'endpoint': endpoint_name,  # Use endpoint_name instead of endpoint
                        'serial_number': connector.get('serial_number', 'UNKNOWN'),
                        'part_number': connector.get('part_number', 'UNKNOWN'),
                        'data': response_data,
                    })
                    logger.debug("Data added to Redis stream %s: %s", redis_stream_key, response_data)
                    break  # Break the retry loop on success
                except redis.RedisError as e:
                    redis_retry_attempts += 1
                    redis_sleep_time = min(redis_max_backoff, (2 ** redis_retry_attempts) * redis_backoff_factor)
                    redis_sleep_time = redis_sleep_time * (0.5 + random.random() / 2)  # Add jitter
                    logger.error("Redis error: %s. Retrying in %.2f seconds.", e, redis_sleep_time)
                    time.sleep(redis_sleep_time)
            # Reset backoff on success
            retry_attempts = 0
        except Exception as e:
            logger.error("Error polling %s: %s", url, e)
            # Exponential backoff with jitter
            retry_attempts += 1
            sleep_time = min(max_backoff, (2 ** retry_attempts) * backoff_factor)
            sleep_time = sleep_time * (0.5 + random.random() / 2)  # Add jitter
            logger.debug("Retrying %s in %.2f seconds", url, sleep_time)
            time.sleep(sleep_time)
            continue  # Skip the rest and retry after backoff
        else:
            # Check stop_event before sleeping
            if stop_event.is_set():
                break

            # Sleep until next poll, including random offset
            elapsed_time = time.time() - start_time
            sleep_time = endpoint_interval - elapsed_time + random.uniform(0, random_offset_max)
            if sleep_time > 0:
                # Use wait instead of sleep to respond to stop_event
                stop_event.wait(sleep_time)

    logger.debug("Polling stopped for endpoint %s", endpoint_name)

def start_poller():
    """
    Start the polling threads based on the current configuration.
    """
    logger.debug("Starting poller with current configuration.")
    global threads
    wall_connectors = config.get('WallConnectors', [])
    for connector in wall_connectors:
        address = connector.get('address')
        if not address:
            logger.debug("Skipping connector with no address.")
            continue
        
        # Get default poll interval
        default_poll_interval = global_config.get('poll_interval', 10)
        
        endpoints = connector.get('endpoints', global_config.get('default_endpoints', []))
        full_address = apply_defaults(address)
        session = requests.Session()
        
        for endpoint_config in endpoints:
            stop_event = threading.Event()
            thread = threading.Thread(
                target=poll_endpoint,
                args=(
                    full_address,
                    endpoint_config,  # Pass the entire endpoint config
                    session,
                    stop_event,
                    default_poll_interval,
                    global_config.get('random_offset_max', 0.1),
                    global_config.get('api_prefix', '/api/1/'),
                    redis_config.get('stream', 'wall_connector_data'),
                    connector
                )
            )
            thread.daemon = True
            threads.append((thread, stop_event))
            thread.start()
            
            # Get endpoint name for logging
            endpoint_name = endpoint_config['endpoint'] if isinstance(endpoint_config, dict) else endpoint_config
            endpoint_interval = endpoint_config.get('interval', default_poll_interval) if isinstance(endpoint_config, dict) else default_poll_interval
            
            logger.info("Started polling thread for %s, endpoint %s with poll interval %s seconds", 
                       full_address, endpoint_name, endpoint_interval)

def stop_poller():
    """
    Stop all polling threads with timeout.
    """
    logger.debug("Stopping all polling threads.")
    global threads
    
    # Set all stop events
    for _, stop_event in threads:
        stop_event.set()
    
    # Join threads with timeout
    for thread, _ in threads:
        thread.join(timeout=2)  # Wait up to 2 seconds per thread
        if thread.is_alive():
            logger.warning("Thread %s did not stop gracefully", thread.name)
    
    # Clear the threads list
    threads.clear()
    logger.info("All polling threads have been stopped.")

def check_redis_connection():
    """
    Check if the Redis connection works before starting.
    If Redis is down, implement aggressive retry logic.
    """
    logger.debug("Checking Redis connection.")
    max_retries = 5
    retry_delay = 2  # Start with 2 seconds
    for attempt in range(max_retries):
        try:
            REDIS_CLIENT.ping()
            logger.info("Successfully connected to Redis.")
            return
        except redis.RedisError as e:
            logger.error("Failed to connect to Redis (attempt %d/%d): %s", attempt + 1, max_retries, e)
            time.sleep(retry_delay)
            # Exponential backoff for retries
            retry_delay *= 2
    # If all retries fail, exit the script
    logger.critical("Exceeded maximum retries to connect to Redis. Exiting.")
    sys.exit(1)

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
        socket_timeout=5  # Timeout for Redis operations
    )
    logger.debug("Redis client setup complete.")

def setup_logging():
    """
    Set up logging based on configuration.
    """
    logger.debug("Setting up logging.")
    log_level_str = global_config.get('log_level', 'INFO').upper()
    numeric_level = getattr(logging, log_level_str, None)
    if not isinstance(numeric_level, int):
        print(f"Invalid log level: {log_level_str}")
        numeric_level = logging.INFO
    logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s')
    logger.debug("Logging setup complete.")

def handle_reload(signum, frame):
    """
    Signal handler for configuration reload.
    """
    logger.info("Received SIGHUP signal, reloading configuration...")
    with config_lock:
        stop_poller()
        load_full_config()
        setup_logging()
        setup_redis()
        check_redis_connection()
        start_poller()
    logger.info("Configuration reloaded successfully.")

def load_full_config():
    """
    Load the full configuration, including Redis and Global settings.
    """
    logger.debug("Loading full configuration.")
    global config
    global redis_config
    global global_config
    config.clear()
    redis_config.clear()
    global_config.clear()
    cfg = load_config(config_file_path)
    config.update(cfg)
    redis_config.update(config.get('Redis', {}))
    global_config.update(config.get('Global', {}))
    logger.debug("Full configuration loaded.")

def parse_args():
    """
    Parse command-line arguments.
    """
    logger.debug("Parsing command-line arguments.")
    parser = argparse.ArgumentParser(description='Wall Connector Poller')
    parser.add_argument('-c', '--config', help='Path to configuration file')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug logging')
    return parser.parse_args()

def main():
    """
    Main function to start the poller.
    """
    logger.debug("Starting main function.")
    global REDIS_CLIENT
    global config_file_path

    # Determine configuration file path
    args = parse_args()
    config_file_path = args.config or os.environ.get('POLLER_CONFIG_PATH') or DEFAULT_CONFIG_PATH

    # Override log level if debug flag is set
    if args.debug:
        global_config['log_level'] = 'DEBUG'
    
    # Set up logging after determining the log level
    setup_logging()

    # Set up signal handler for SIGHUP
    signal.signal(signal.SIGHUP, handle_reload)

    # Load configuration
    with config_lock:
        load_full_config()
        logger.info("Using configuration file: %s", config_file_path)
        setup_redis()
        check_redis_connection()
        start_poller()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down poller...")
        with config_lock:
            stop_poller()
        logger.info("Shutdown complete")
        sys.exit(0)
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        with config_lock:
            stop_poller()
        sys.exit(1)

if __name__ == '__main__':
    main()
