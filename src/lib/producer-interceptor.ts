import crypto from "node:crypto";

type OutgoingMessage = {
  key?: string;
  value: Buffer | string;
  headers?: Record<string, string>;
};

export function interceptMessage(message: OutgoingMessage): OutgoingMessage {
  const now = new Date().toISOString();
  console.log("=========================");
  console.log("message intercepted");
  console.log(message);
  console.log("=========================");
  return {
    ...message,
    headers: {
      ...message.headers,
      "x-produced-at": now,
      "x-interceptor": "custom-wrapper",
      "x-trace-id": crypto.randomUUID(),
    },
  };
}
