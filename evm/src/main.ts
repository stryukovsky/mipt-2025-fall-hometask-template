import {runClickhouseProcessing} from 'core/clickhouse-processor'
import {Bytes20, Bytes32} from 'core/portal/data'
import {PortalDataSource} from 'core/portal/data-source'
import * as erc20 from './abi/erc20'


/**
 * All USDC transfers since inception
 */
const source = new PortalDataSource(
    'https://portal.sqd.dev/datasets/ethereum-mainnet/finalized-stream',
    {
        type: 'evm',
        fromBlock: 6_082_465, // Contract creation block
        fields: {
            block: {
                timestamp: true
            },
            log: {
                logIndex: true,
                transactionHash: true,
                address: true,
                topics: true,
                data: true
            }
        },
        logs: [
            {
                address: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'],
                topic0: [erc20.events.Transfer.topic]
            }
        ]
    }
)


runClickhouseProcessing({
    clickhouse: 'http://default:123@localhost:8123',
    clickhouseDatabase: 'erc20_src',
    source,
    map(block): Data {
        let data = new Data()
        for (let log of (block.logs ?? [])) {
            if (erc20.events.Transfer.is(log)) {
                let {from, to, value} = erc20.events.Transfer.decode(log)

                let common = {
                    log_index: log.logIndex,
                    transaction_hash: log.transactionHash,
                    contract: log.address,
                }

                if (from === to) {
                    // transfer from self to self does not update the balance
                    data.balance_updates.push({
                        ...common,
                        account: from,
                        counterparty: to,
                        amount: "0"
                    })
                } else {
                    data.balance_updates.push({
                        ...common,
                        account: from,
                        counterparty: to,
                        amount: (-value).toString()
                    })

                    data.balance_updates.push({
                        ...common,
                        account: to,
                        counterparty: from,
                        amount: value.toString()
                    })
                }
            }
        }
        return data
    }
})


class Data {
    balance_updates: BalanceUpdate[] = []
}


interface BalanceUpdate {
    log_index: number
    transaction_hash: Bytes32
    contract: Bytes20
    account: Bytes20
    counterparty: Bytes20
    amount: string
}
