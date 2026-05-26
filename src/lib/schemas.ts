export const userCreatedSchema = {
  type: "record",
  name: "UserCreated",
  namespace: "kafkabook.chapter1",
  fields: [
    { name: "id", type: "string" },
    { name: "name", type: "string" },
    { name: "createdAt", type: "string" },
  ],
} as const;
