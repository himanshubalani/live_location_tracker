import { Kafka } from 'kafkajs';

export const kafkaClient = new Kafka({
  clientId: 'himanshu',
  brokers: ['0.0.0.0:9092'],
});
