import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { env } from "../config/env.js";

export const registry = new SchemaRegistry({
  host: env.schemaRegistryUrl,
});
