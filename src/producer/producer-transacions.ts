import { kafka } from "../lib/kafka.js";
import { TOPICS } from "../lib/topics.js";
import { customPartitioner } from "../lib/partitioner.js";
import { interceptMessage } from "../lib/producer-interceptor.js";

async function main() {
  const producer = kafka.producer({
    createPartitioner: customPartitioner,
    idempotent: true,
    maxInFlightRequests: 5,
  });

  try {
    await producer.connect();

    const rawMessage = {
      key: "user-" + new Date().toISOString().split("T")[0],
      value: JSON.stringify({
        id: "user-" + new Date().toISOString().split("T")[0],
        action: "created",
        at: new Date().toISOString(),
      }),
      headers: {
        priority: "HIGH",
        source: "kafka-book-lab",
        "content-type": "application/json",
      },
    };

    const message = interceptMessage(rawMessage);

    const result = await producer.send({
      topic: TOPICS.chapter1,
      messages: [message],
    });
    console.log("=========================");
    console.log("result");
    console.log(result);
    console.log("=========================");
    console.log("Sent message with headers:", message.headers);
  } finally {
    await producer.disconnect();
  }
}

main().catch((error) => {
  console.error("Error producing message", error);
  process.exit(1);
});
