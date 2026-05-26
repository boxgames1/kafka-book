import { kafka } from "../lib/kafka.js";
import { env } from "../config/env.js";
import { TOPICS } from "../lib/topics.js";
import {
  AssignerProtocol,
  Assignment,
  Cluster,
  Consumer,
  PartitionAssigner,
  PartitionAssigners,
} from "kafkajs";

function decodeHeaders(headers?: Record<string, Buffer>) {
  if (!headers) return {};

  return Object.fromEntries(
    Object.entries(headers).map(([key, value]) => [key, value?.toString()]),
  );
}

const MyPartitionAssigner: PartitionAssigner = ({
  cluster,
}: {
  cluster: Cluster;
}) => ({
  name: "MyPartitionAssigner",
  version: 1,
  protocol({ topics }: { topics: string[] }) {
    return {
      name: this.name,
      metadata: AssignerProtocol.MemberMetadata.encode({
        version: this.version,
        topics,
        userData: Buffer.from("my-user-data"),
      }),
    };
  },
  async assign({ members, topics }) {
    return members.map((member: any) => ({
      memberId: member.id,
      memberAssignment: AssignerProtocol.MemberAssignment.encode({
        version: this.version,
        assignment: Object.fromEntries(
          topics.map((topic: string) => [topic, [0]]),
        ) as any,
        userData: Buffer.from("my-user-data"),
      }),
    }));
  },
});

async function main() {
  const consumer = kafka.consumer({
    groupId: env.kafkaGroupId,
    partitionAssigners: [PartitionAssigners.roundRobin, MyPartitionAssigner],
  });

  await consumer.connect();
  await consumer.subscribe({
    topic: TOPICS.chapter1,
    fromBeginning: true,
  });

  //consumer.seek({ topic: TOPICS.chapter1, partition: 0, offset: "0" }); // to start from the beginning

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString(),
        value: message.value?.toString(),
        headers: decodeHeaders(message.headers as Record<string, Buffer>),
      });
      consumer.commitOffsets([{ topic, partition, offset: message.offset }]);
    },
  });
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
