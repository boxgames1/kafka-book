import { Kafka, logLevel } from "kafkajs";

const kafka = new Kafka({
  clientId: "stock-stats-producer",
  brokers: ["localhost:9092"],
  logLevel: logLevel.INFO,
});

const producer = kafka.producer();
const topic = "trades-input";

async function main() {
  await producer.connect();

  const samples = [
    { ticker: "AAPL", askPrice: 150.0 },
    { ticker: "AAPL", askPrice: 149.5 },
    { ticker: "MSFT", askPrice: 300.0 },
    { ticker: "MSFT", askPrice: 301.2 },
    { ticker: "GOOGL", askPrice: 120.0 },
  ];

  for (const trade of samples) {
    await producer.send({
      topic,
      messages: [{ key: trade.ticker, value: JSON.stringify(trade) }],
    });
    console.log("sent", trade);
  }

  await producer.disconnect();
}

main().catch(async (err) => {
  console.error(err);
  await producer.disconnect().catch(() => {});
  process.exit(1);
});
