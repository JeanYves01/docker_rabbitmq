// const express = require('express');
// const amqp = require('amqplib');
// const mysql = require('mysql2');
// const winston = require('winston');

// const app = express();
// const port = 3002;

// const RABBITMQ_HOST = 'rabbitmq';
// const RABBITMQ_USER = 'guest';
// const RABBITMQ_PASS = 'guest';
// const RABBITMQ_QUEUE = 'json_queue';

// const DB_HOST = 'mysql';
// const DB_NAME = 'suivi_conso';
// const DB_USER = 'jeanyves';
// const DB_PASS = '01@3338689';
// const DB_PORT = '5000';

// // Configurer le logging
// const logger = winston.createLogger({
//     level: 'info',
//     format: winston.format.combine(
//         winston.format.timestamp(),
//         winston.format.printf(({ timestamp, level, message }) => `${timestamp} - ${level.toUpperCase()} - ${message}`)
//     ),
//     transports: [new winston.transports.Console()]
// });

// async function waitForService(serviceFunc, retries = 50, delay = 10000) {
//     for (let i = 0; i < retries; i++) {
//         try {
//             return await serviceFunc();
//         } catch (e) {
//             logger.info(`Waiting for service: ${e.message}`);
//             await new Promise(resolve => setTimeout(resolve, delay));
//         }
//     }
//     throw new Error(`Service not available after ${retries * (delay / 1000)} seconds`);
// }

// async function connectRabbitMQ() {
//     const connection = await amqp.connect({
//         protocol: 'amqp',
//         hostname: RABBITMQ_HOST,
//         username: RABBITMQ_USER,
//         password: RABBITMQ_PASS
//     });
//     const channel = await connection.createChannel();
//     await channel.assertQueue(RABBITMQ_QUEUE);
//     return channel;
// }

// async function connectDB() {
//     const connection = mysql.createConnection({
//         host: DB_HOST,
//         user: DB_USER,
//         password: DB_PASS,
//         database: DB_NAME,
//         port: DB_PORT
//     });
//     await connection.promise().execute(`CREATE TABLE IF NOT EXISTS messages
//                                         (id INT AUTO_INCREMENT PRIMARY KEY,
//                                         content JSON NOT NULL)`);
//     logger.info("Table 'messages' is ready");
//     return connection;
// }

// async function saveMessageToDB(dbConn, message) {
//     try {
//         const [rows] = await dbConn.promise().execute('INSERT INTO messages (content) VALUES (?)', [message]);
//         if (rows.affectedRows > 0) {
//             logger.info("Message saved to database");
//         } else {
//             logger.error("Message not saved to database");
//         }
//         const [result] = await dbConn.promise().query('SELECT * FROM messages ORDER BY id DESC LIMIT 1');
//         if (result.length > 0) {
//             logger.info(`Message retrieved from database: ${JSON.stringify(result[0])}`);
//         } else {
//             logger.error("Message not found in database after insertion");
//         }
//     } catch (err) {
//         logger.error(`Error: ${err.message}`);
//     }
// }

// async function consumeMessages(rabbitmqChannel, dbConn) {
//     rabbitmqChannel.consume(RABBITMQ_QUEUE, async (msg) => {
//         if (msg !== null) {
//             const message = msg.content.toString();
//             logger.info(`Received message: ${message}`);
//             await saveMessageToDB(dbConn, message);
//             rabbitmqChannel.ack(msg);
//         }
//     });
//     logger.info("Waiting for messages. To exit press CTRL+C");
// }

// (async function main() {
//     const rabbitmqChannel = await waitForService(connectRabbitMQ);
//     const dbConn = await waitForService(connectDB);
//     try {
//         await consumeMessages(rabbitmqChannel, dbConn);
//     } catch (err) {
//         logger.error(`Error: ${err.message}`);
//         dbConn.end();
//         rabbitmqChannel.close();
//     }
// })();

// app.listen(port, () => {
//     logger.info(`Server running on http://localhost:${port}`);
// });
