import { Kafka, logLevel } from "kafkajs";

const kafka = new Kafka({
  clientId: "stock-stats-output-consumer",
  brokers: ["localhost:9092"],
  logLevel: logLevel.INFO,
});

const consumer = kafka.consumer({ groupId: "stock-stats-output-group" });

async function main() {
  await consumer.connect();
  await consumer.subscribe({
    topic: "trades-stats-output",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const value = message.value?.toString("utf8");
      if (value) console.log(value);
    },
  });
}

main().catch(async (err) => {
  console.error(err);
  await consumer.disconnect().catch(() => {});
  process.exit(1);
});
