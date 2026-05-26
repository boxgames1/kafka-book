import { Kafka } from "kafkajs";
import "dotenv/config";

const brokers = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((s) => s.trim());

export const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "kafka-lab",
  brokers,
});
