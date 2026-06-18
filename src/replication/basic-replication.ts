import { Kafka } from "kafkajs";

const sourceKafka = new Kafka({ brokers: ["localhost:9092"] });
const targetKafka = new Kafka({ brokers: ["localhost:9092"] });

const consumer = sourceKafka.consumer({ groupId: "oreilly-group" });
const producer = targetKafka.producer();

async function replicate() {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: "oreilly-test", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("=========================");
      console.log("topic");
      console.log(topic);
      console.log(partition);
      console.log(message);
      console.log("=========================");
      await producer.send({
        topic: topic,
        messages: [{ key: message.key, value: message.value }],
      });
    },
  });
}

replicate().catch(console.error);
