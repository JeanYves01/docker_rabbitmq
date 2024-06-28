import os
import json
import pika
import mysql.connector
from time import sleep
import logging

# Configurer le logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_USER = 'guest'
RABBITMQ_PASS = 'guest'
RABBITMQ_QUEUE = 'json_queue'

DB_HOST = 'mysql'
DB_NAME = 'suivi_conso'
DB_USER = 'jeanyves'
DB_PASS = '01@3338689'
DB_PORT = '3306'


def wait_for_service(service_func, retries=50, delay=10):
    for i in range(retries):
        try:
            return service_func()
        except Exception as e:
            logging.info(f"Waiting for service: {e}")
            sleep(delay)
    raise Exception(f"Service not available after {retries * delay} seconds")

def connect_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE)
    return channel

def connect_db():
    conn = mysql.connector.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS messages
                      (id INT AUTO_INCREMENT PRIMARY KEY,
                       content JSON NOT NULL)''')
    conn.commit()
    return conn


def save_message_to_db(db_conn, message):
    try:
        cursor = db_conn.cursor()

        # Vérifier dans quelle base de données on travaille
        cursor.execute('SELECT DATABASE();')
        current_database = cursor.fetchone()
        if current_database:
            logging.info(f"Using database: {current_database[0]}")
        else:
            logging.error("Unable to determine the current database")

        # Insérer le message
        query = 'INSERT INTO suivi_conso.messages (content) VALUES (%s)'
        cursor.execute(query, (message,))
        db_conn.commit()
        logging.info("Message saved to database")

        # Validation: récupérer les données après insertion pour vérifier
        cursor.execute('SELECT * FROM suivi_conso.messages ORDER BY id DESC LIMIT 1;')
        result = cursor.fetchone()
        if result:
            logging.info(f"Message retrieved from database: {result}")
        else:
            logging.error("Message not found in database after insertion")
    except mysql.connector.Error as err:
        logging.error(f"Error: {err}")
        db_conn.rollback()


# def save_message_to_db(db_conn, message):
#     try:
#         cursor = db_conn.cursor()
#         query = 'INSERT INTO suivi_conso.messages (content) VALUES (%s)'
#         cursor.execute(query, (message,))
#         db_conn.commit()
#         logging.info("Message saved to database")

#         # Validation: récupérer les données après insertion pour vérifier
#         cursor.execute('SELECT * FROM suivi_conso.messages;', )
#         result = cursor.fetchone()
#         if result:
#             logging.info(f"Message retrieved from database: {result}")
#         else:
#             logging.error("Message not found in database after insertion")
#     except mysql.connector.Error as err:
#         logging.error(f"Error: {err}")
#         db_conn.rollback()

def consume_messages(rabbitmq_channel, db_conn):
    def callback(ch, method, properties, body):
        message = body.decode()
        logging.info(f"Received message: {message}")
        save_message_to_db(db_conn, message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    rabbitmq_channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
    logging.info("Waiting for messages. To exit press CTRL+C")
    rabbitmq_channel.start_consuming()

if __name__ == '__main__':
    rabbitmq_channel = wait_for_service(connect_rabbitmq)
    db_conn = wait_for_service(connect_db)
    try:
        consume_messages(rabbitmq_channel, db_conn)
    except KeyboardInterrupt:
        db_conn.close()
        rabbitmq_channel.close()
