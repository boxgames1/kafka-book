import { Kafka, logLevel } from "kafkajs";
import "dotenv/config";

const brokers = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((s) => s.trim());

export const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "kafka-lab",
  brokers,
  logCreator:
    (name) =>
    (...args) =>
      console.log(`[${name}]`, ...args),
  logLevel: logLevel.DEBUG,
  sasl: {
    mechanism: "PLAIN",
    authenticationProvider: ({ host, port, logger, saslAuthenticate }) => {
      return {
        authenticate: async () => {
          // 1. Construct or fetch your authentication token/data
          const myToken = Buffer.from("my-auth-payload");

          // 2. Perform the SASL handshake with the broker
          await saslAuthenticate({
            request: {
              encode: () => myToken,
            },
            response: {
              decode: (rawResponse) => rawResponse,
              parse: (data) => data.toString(),
            },
          });
        },
      };
    },
  },
  ssl: true,
});
