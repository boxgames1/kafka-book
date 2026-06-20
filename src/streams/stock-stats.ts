import { Kafka, logLevel } from "kafkajs";

type Trade = {
  ticker: string;
  askPrice: number;
};

type TradeStats = {
  bestAskPrice: number | null;
  tradeCount: number;
  sumAskPrice: number;
  avgAskPrice: number | null;
};

type OutputRow = {
  ticker: string;
  windowStart: number;
  windowEnd: number;
  bestAskPrice: number | null;
  tradeCount: number;
  avgAskPrice: number | null;
};

const BROKERS = ["localhost:9092"];
const CLIENT_ID = "stock-stats-js";
const GROUP_ID = "stock-stats-group";
const INPUT_TOPIC = "trades-input";
const OUTPUT_TOPIC = "trades-stats-output";
const WINDOW_SIZE_MS = 5_000;
const RETENTION_MS = 60_000;

const kafka = new Kafka({
  clientId: CLIENT_ID,
  brokers: BROKERS,
  logLevel: logLevel.INFO,
});

const consumer = kafka.consumer({ groupId: GROUP_ID });
const producer = kafka.producer();

const state = new Map<string, TradeStats>();

function initStats(): TradeStats {
  return {
    bestAskPrice: null,
    tradeCount: 0,
    sumAskPrice: 0,
    avgAskPrice: null,
  };
}

function updateStats(agg: TradeStats, price: number): TradeStats {
  agg.tradeCount += 1;
  agg.sumAskPrice += price;
  agg.bestAskPrice =
    agg.bestAskPrice === null ? price : Math.min(agg.bestAskPrice, price);
  agg.avgAskPrice = agg.sumAskPrice / agg.tradeCount;
  return agg;
}

function windowStartFor(timestampMs: number) {
  return Math.floor(timestampMs / WINDOW_SIZE_MS) * WINDOW_SIZE_MS;
}

function makeStateKey(ticker: string, windowStartMs: number) {
  return `${ticker}|${windowStartMs}`;
}

function toOutput(
  ticker: string,
  windowStartMs: number,
  stats: TradeStats,
): OutputRow {
  return {
    ticker,
    windowStart: windowStartMs,
    windowEnd: windowStartMs + WINDOW_SIZE_MS,
    bestAskPrice: stats.bestAskPrice,
    tradeCount: stats.tradeCount,
    avgAskPrice: stats.avgAskPrice,
  };
}

function cleanupExpired(nowMs: number) {
  for (const key of state.keys()) {
    const [, windowStartStr] = key.split("|");
    const windowStartMs = Number(windowStartStr);
    if (nowMs - windowStartMs > RETENTION_MS) {
      state.delete(key);
    }
  }
}

async function main() {
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString("utf8");
      if (!raw) return;

      let trade: Trade;
      try {
        trade = JSON.parse(raw);
      } catch {
        return;
      }

      if (!trade.ticker || typeof trade.askPrice !== "number") return;

      const timestampMs = message.timestamp
        ? Number(message.timestamp)
        : Date.now();
      const windowStartMs = windowStartFor(timestampMs);
      const key = makeStateKey(trade.ticker, windowStartMs);

      const agg = state.get(key) ?? initStats();
      updateStats(agg, trade.askPrice);
      state.set(key, agg);

      const output = toOutput(trade.ticker, windowStartMs, agg);

      console.log(JSON.stringify(output));

      await producer.send({
        topic: OUTPUT_TOPIC,
        messages: [
          {
            key: `${trade.ticker}|${windowStartMs}`,
            value: JSON.stringify(output),
          },
        ],
      });

      cleanupExpired(Date.now());
    },
  });

  process.on("SIGINT", async () => {
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(0);
  });

  process.on("SIGTERM", async () => {
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(0);
  });
}

main().catch(async (err) => {
  console.error(err);
  await consumer.disconnect().catch(() => {});
  await producer.disconnect().catch(() => {});
  process.exit(1);
});
