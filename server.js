const { Kafka } = require('kafkajs');
const express = require('express');
const socketIo = require('socket.io');

const runApp = async () => {

  const username = 'M6NWCUNX664BFIYJ';
  const password = 'X5o5hVeg+kAy4w78Upr7Yu/b1l08Q+TKD/WIAnvC+mtvMnmU9V7Y67J5d8woac+u';
  const broker = 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092';
  const sasl = username && password ? { username, password, mechanism: 'plain' } : null;
  const ssl = !!sasl;
  const kafkaSettings = {
      topics: ['topic_0'], //create a topic in kafka
      clientId: 'click-analytics', //appname?
      brokers: [broker],
      ssl,
      sasl,
      connectionTimeout: 3000,
      authenticationTimeout: 1000,
      reauthenticationThreshold: 10000,
  };

  const kafka = new Kafka(kafkaSettings);

  const producer = kafka.producer();

  await producer.connect()
  await producer.send({
    topic: 'topic_0',
    messages: [
      // { key: 'key1', value: 'hello world', partition: 0 },
      // { value: 'Hello KafkaJS user!' },
    ],
  })

  await producer.disconnect()

  const consumer = kafka.consumer({ groupId: 'test-group' })

  await consumer.connect()
  await consumer.subscribe({ topic: 'topic_0', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: "In Consumer: " + message.value.toString(),
      })
    },
  })

  const port = 3000;
  const app = express();

  const server = app.listen(port, () => {
    console.log(`Listening on port ${server.address().port}`);
  });
  const io = socketIo(server, {
    cors: {
      origin: '*',
    }
  });

  io.on('connection', client => {
    console.log('Connected', client);

    client.on('event', data => { 
      console.log('Event triggered by client')
    });

    client.on('disconnect', () => { 
      console.log('Client disconnected');
    });
  });

}

runApp();

//to-do:
/*
- resolve the error
 1. Do we have cluster turn on?
 2. Do we have the right data shape?
 3. Do we have data generator?
 */