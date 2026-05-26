import { kafka } from "../lib/kafka.js";
import { TOPICS } from "../lib/topics.js";

const main = async () => {
  const admin = kafka.admin();
  await admin.connect();
  /* await admin.createTopics({
    topics: [
      {
        topic: TOPICS.chapter1 + "-new",
        numPartitions: 1,
        replicationFactor: 1,
      },
    ],
  }); 
  /*await admin.deleteTopics({
    topics: [TOPICS.chapter1 + "-new"],
  }); */
  const topics = await admin.listTopics();

  const topicMetadata = await admin.fetchTopicMetadata({
    topics: [TOPICS.chapter1 + "-new"],
  });

  const consumerGroups = await admin.listGroups();
  const offsets = await admin.fetchOffsets({
    groupId: "chapter-1-group",
    topics: [TOPICS.chapter1 + "-new"],
  });

  await admin.createPartitions({
    topicPartitions: [
      {
        topic: TOPICS.chapter1 + "-new",
        count: 5,
      },
    ],
  });

  const clusterMetadata = await admin.describeCluster();
  console.log(clusterMetadata);

  const topicPartitions = await admin.listPartitionReassignments({
    topics: [
      {
        topic: TOPICS.chapter1 + "-new",
        partitions: [0, 1, 2, 3],
      },
    ],
  });
  console.log(topicPartitions);
  console.log(offsets);
  console.log(consumerGroups);
  console.log(topicMetadata);
  console.log(topicMetadata.topics[0]?.partitions);
  console.log(topics);

  await admin.disconnect();
};

main().catch((error) => {
  console.error("Error creating topic", error);
  process.exit(1);
});
