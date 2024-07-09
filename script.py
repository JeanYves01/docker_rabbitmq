import os
import json
import pika
import paramiko
from time import sleep
import logging

SFTP_HOST = 'sftp'
SFTP_PORT = 22
SFTP_USER = 'ftpuser'
SFTP_PASS = 'abcd1234'
SFTP_DIR = '/ftp'

RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_USER = 'guest'
RABBITMQ_PASS = 'guest'
RABBITMQ_QUEUE = 'json_queue'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def wait_for_service(service_func, retries=50, delay=10):
    for i in range(retries):
        try:
            return service_func()
        except Exception as e:
            logging.info(f"Waiting for service: {e}")
            sleep(delay)
    raise Exception(f"Service not available after {retries * delay} seconds")

def connect_sftp():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp

def connect_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE)
    return channel

def send_test_message(rabbitmq_channel):
    message = {"key": "test"}
    message = {"nom": "Jean Yves"}
    rabbitmq_channel.basic_publish(exchange='',
                                   routing_key=RABBITMQ_QUEUE,
                                   body=json.dumps(message))
    logging.info(f"Sent test message: {message}")

def watch_sftp_directory(sftp, rabbitmq_channel):
    while True:
        try:
            logging.info(f"Checking directory: {SFTP_DIR}")
            for filename in sftp.listdir(SFTP_DIR):
                logging.info(f"Found file: {filename}")
                if filename.endswith('.json'):
                    filepath = os.path.join(SFTP_DIR, filename)
                    with sftp.open(filepath, 'r') as f:
                        data = json.load(f)
                        rabbitmq_channel.basic_publish(exchange='',
                                                       routing_key=RABBITMQ_QUEUE,
                                                       body=json.dumps(data))
                    logging.info(f"Processed and removed file: {filename}")
                    sftp.remove(filepath)
        except Exception as e:
            logging.error(f"Error: {e}")
        sleep(10)

if __name__ == '__main__':
    sftp = wait_for_service(connect_sftp)
    rabbitmq_channel = wait_for_service(connect_rabbitmq)
    send_test_message(rabbitmq_channel)
    try:
        watch_sftp_directory(sftp, rabbitmq_channel)
    except KeyboardInterrupt:
        sftp.close()
        rabbitmq_channel.close()
