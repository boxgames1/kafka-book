import { kafka } from "../lib/kafka.js";
import { TOPICS } from "../lib/topics.js";

async function main() {
  const admin = kafka.admin();

  try {
    await admin.connect();

    const created = await admin.createTopics({
      topics: [
        {
          topic: TOPICS.chapter1,
          numPartitions: 1,
          replicationFactor: 1,
        },
      ],
    });

    console.log(
      created
        ? `Topic created: ${TOPICS.chapter1}`
        : `Topic already exists: ${TOPICS.chapter1}`,
    );
  } finally {
    await admin.disconnect();
  }
}

main().catch((error) => {
  console.error("Error creating topic", error);
  process.exit(1);
});
