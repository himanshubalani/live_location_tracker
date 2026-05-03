import { Kafka } from 'kafkajs';

export const kafkaClient = new Kafka({
  clientId: 'location-tracker',
  brokers:[process.env.KAFKA_BOOTSTRAP_SERVER], // E.g. 'xyz.gcp.com:9092'
  ssl: true,
  sasl: {
    mechanism: 'plain', 
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});