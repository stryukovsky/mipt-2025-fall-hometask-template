import { runClickhouseProcessing } from "core/clickhouse-processor";
import { Bytes20 } from "core/portal/data";
import { PortalDataSource } from "core/portal/data-source";
import Decimal from "decimal.js";

const SLOT_LIQUIDITY =
  "0x0000000000000000000000000000000000000000000000000000000000000004";
const SLOT_0 =
  "0x0000000000000000000000000000000000000000000000000000000000000000";
const source = new PortalDataSource(
  "https://portal.sqd.dev/datasets/ethereum-mainnet/finalized-stream",
  {
    type: "evm",
    fromBlock: 23034325, // Contract creation block
    fields: {
      block: {
        timestamp: true,
      },
      stateDiff: {
        address: true,
        key: true,
        kind: true,
        prev: true,
        next: true,
      },
    },
    stateDiffs: [
      {
        address: ["0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8"],
        key: [SLOT_LIQUIDITY, SLOT_0],
        // key: ["liquidity"],
        // kind: ["*"],
      },
    ],
  },
);

function parsePrice(slot0Hex: string): string {
  // Remove 0x prefix if present
  const hex = slot0Hex.startsWith("0x") ? slot0Hex.slice(2) : slot0Hex;

  // Pad to 64 characters (32 bytes) if needed
  const paddedHex = hex.padStart(64, "0");

  // Extract sqrtPriceX96 (first 160 bits = 20 bytes = 40 hex chars)
  // Note: We need to read from the right side due to little-endian storage
  const sqrtPriceX96Hex = paddedHex.slice(-40); // Last 40 chars (20 bytes)
  const sqrtPriceX96 = BigInt("0x" + sqrtPriceX96Hex);
  const Q96 = Decimal(2).pow(96);
  // const Q192 = Q96 * Q96;

  const numerator = sqrtPriceX96;

  const price = Decimal(numerator).div(Decimal(Q96));

  return price.pow(2).toString();
}

runClickhouseProcessing({
  clickhouse: "http://default:123@localhost:8123",
  clickhouseDatabase: "u3_tvl_src",
  source,
  map(block): Data {
    let data = new Data();
    if (block.stateDiffs !== undefined) {
      block.stateDiffs.forEach((entry) => {
        if (entry.key == SLOT_LIQUIDITY) {
          const amount = BigInt(entry.next!).toString();
          const result = {
            block_number: block.header.number,
            block_timestamp: block.header.timestamp,
            pool: entry.address,
            amount,
          };
          console.log(result);
          data.liquidity.push(result);
        } else if (entry.key == SLOT_0) {
          const priceValue = parsePrice(entry.next!);

          const result = {
            block_number: block.header.number,
            block_timestamp: block.header.timestamp,
            pool: entry.address,
            price: priceValue,
          };
          data.price.push(result);
        } else {
          console.warn(`Unknown entry key: ${entry.key}`);
        }
      });
    }
    return data;
  },
});

class Data {
  liquidity: PoolRawLiquidity[] = [];
  price: PoolRawPrice[] = [];
}

interface PoolRawLiquidity {
  block_number: number;
  block_timestamp: number;
  pool: Bytes20;
  amount: string;
}

interface PoolRawPrice {
  block_number: number;
  block_timestamp: number;
  pool: Bytes20;
  price: string;
}

interface TVLEntry {
  block_number: number;
  block_timestamp: number;
  pool: Bytes20;
  token0: Bytes20;
  token1: Bytes20;
  amount0: string;
  amount1: string;
}
