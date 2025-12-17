import { runClickhouseProcessing } from "core/clickhouse-processor";
import { Bytes20 } from "core/portal/data";
import { PortalDataSource } from "core/portal/data-source";

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
        key: [
          "0x0000000000000000000000000000000000000000000000000000000000000004",
        ],
        // key: ["liquidity"],
        // kind: ["*"],
      },
    ],
  },
);

runClickhouseProcessing({
  clickhouse: "http://default:123@localhost:8123",
  clickhouseDatabase: "u3_tlv_src",
  source,
  map(block): Data {
    let data = new Data();
    if (block.stateDiffs !== undefined) {
      const liquidityAtTheEndRaw = block.stateDiffs.map(
        (entry) => entry.next,
      )[0];

      if (liquidityAtTheEndRaw !== undefined) {
        BigInt(liquidityAtTheEndRaw!!);
      }
      console.log(JSON.stringify(block.stateDiffs));
    }
    // for (let log of block.logs ?? []) {
    //   if (erc20.events.Transfer.is(log)) {
    //     let { from, to, value } = erc20.events.Transfer.decode(log);
    //
    //     let common = {
    //       log_index: log.logIndex,
    //       transaction_hash: log.transactionHash,
    //       contract: log.address,
    //     };
    //
    //     if (from === to) {
    //       // transfer from self to self does not update the balance
    //       data.balance_updates.push({
    //         ...common,
    //         account: from,
    //         counterparty: to,
    //         amount: "0",
    //       });
    //     } else {
    //       data.balance_updates.push({
    //         ...common,
    //         account: from,
    //         counterparty: to,
    //         amount: (-value).toString(),
    //       });
    //
    //       data.balance_updates.push({
    //         ...common,
    //         account: to,
    //         counterparty: from,
    //         amount: value.toString(),
    //       });
    //     }
    //   }
    // }
    return data;
  },
});

class Data {
  // liquidity_change: TVLEntry[] = [];
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
