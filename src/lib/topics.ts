import { env } from "../config/env.js";

export const TOPICS = {
  chapter1: env.kafkaTopic,
} as const;
