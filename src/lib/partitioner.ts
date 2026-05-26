import type { ICustomPartitioner, PartitionerArgs } from "kafkajs";

function getPriority(args: PartitionerArgs): string {
  const value = args.message.headers?.priority;

  if (value == null) return "";
  if (Array.isArray(value)) {
    const first = value[0];
    if (first == null) return "";
    return Buffer.isBuffer(first) ? first.toString("utf8") : first;
  }

  return Buffer.isBuffer(value) ? value.toString("utf8") : value;
}

export const customPartitioner: ICustomPartitioner = () => {
  return ({ partitionMetadata, ...rest }: PartitionerArgs): number => {
    const priority = getPriority({ partitionMetadata, ...rest }).toUpperCase();

    if (priority === "HIGH") return 0;
    if (priority === "MEDIUM") return Math.min(1, partitionMetadata.length - 1);
    return Math.min(2, partitionMetadata.length - 1);
  };
};
