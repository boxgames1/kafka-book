import { Kafka } from "kafkajs";
import http from "http";

const kafka = new Kafka({
  clientId: "wordcount-js",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "wordcount-group" });
const producer = kafka.producer();

const counts = new Map(); // word -> long

// Topology-like pipeline (manual, no StreamsBuilder DSL)
function processLine(text) {
  // mapValues: lowercase
  const lower = text.toLowerCase();

  // flatMapValues: split on non-word characters
  const words = lower.match(/\b[\p{L}\p{N}']+\b/gu) || [];

  // filter: remove empty words
  return words.filter((w) => w.length > 0);
}

async function main() {
  await consumer.connect();
  await producer.connect();

  // Source: builder.stream("word-count-input")
  await consumer.subscribe({
    topic: "word-count-input",
    fromBeginning: true,
  });

  // Run consumer with eachMessage
  await consumer.run({
    eachMessage: async ({ message }) => {
      const text = message.value?.toString("utf8") || "";

      // Process through pipeline: map → flatMap → filter
      const words = processLine(text);

      // groupBy + count: aggregate in memory
      for (const word of words) {
        const nextCount = (counts.get(word) || 0) + 1;
        counts.set(word, nextCount);

        // to("word-count-output")
        await producer.send({
          topic: "word-count-output",
          messages: [
            {
              key: word,
              value: JSON.stringify({ word, count: nextCount }),
            },
          ],
        });
      }
    },
  });

  // Shutdown hook: close streams on exit
  process.on("SIGINT", async () => {
    console.log("Shutting down stream");
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(0);
  });

  // "while true" + sleep loop (like Java)
  while (true) {
    console.log("Current counts:", Object.fromEntries(counts));
    await new Promise((res) => setTimeout(res, 5000));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
