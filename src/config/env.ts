import "dotenv/config";

function required(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;

  if (!value) {
    throw new Error(`Missing env var: ${name}`);
  }

  return value;
}

export const env = {
  kafkaClientId: required("KAFKA_CLIENT_ID", "kafka-lab"),
  kafkaBrokers: required("KAFKA_BROKERS", "localhost:9092")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean),
  kafkaTopic: required("KAFKA_TOPIC", "chapter-1-test"),
  kafkaGroupId: required("KAFKA_GROUP_ID", "chapter-1-group"),
  schemaRegistryUrl: required("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
};
