import {createLogger} from '@subsquid/logger'
import {Speed} from '@subsquid/util-internal-counters'
import {ClickhouseClient} from './clickhouse/client'
import {BlockWriter, TableOptions} from './clickhouse/writer'
import type {BlockBase, BlockRef} from './common'
import {groupBy, last, maybeLast, runProgram} from './util/misc'
import {Timer} from './util/timer'


const log = createLogger('core:clickhouse-processor')


export interface DataBatch<B> {
    blocks: B[]
    headNumber?: number
}


export interface DataSource<B> {
    createDataStream(afterBlock?: BlockRef): AsyncIterable<DataBatch<B>>
}


export type GetDataSourceBlock<S> = S extends DataSource<infer B> ? B : never


export interface ProcessorArgs<B, R> {
    /**
     * URL of the ClickHouse HTTP API.
     */
    clickhouse: string
    /**
     * Database to store the resulting data in.
     */
    clickhouseDatabase: string
    /**
     * Write options per table.
     */
    clickhouseTables?: Record<string, TableOptions>
    /**
     * Data source.
     */
    source: DataSource<B>
    /**
     * Data mapping function.
     */
    map: (block: B) => R | Promise<R>
}


/**
 * Run data processing.
 *
 * This function works as the program entry point,
 * it terminates the entire node process as soon as data processing terminates.
 *
 * Data processing comprises the following parts:
 *
 * 1. {@link DataSource} (passed as `args.source`) — responsible for fetching data starting from a given block
 * 2. Data mapping function (passed as `args.map`) — responsible for mapping a block into
 *    a set of rows to insert into the target database
 * 3. ClickHouse database ([a unit](https://clickhouse.com/docs/sql-reference/statements/create/database))
 *    (passed as `args.clickhouseDatabase`).
 *
 * The provided framework does not allow users to execute arbitrary DML statements;
 * all produced data is append-only.
 *
 * The target database must have a certain structure.
 *
 * First, there must be a `blocks` table:
 *
 * ```sql
 * CREATE TABLE blocks (
 *     number UInt64, -- or UInt32
 *     hash String, -- or FixedString(N)
 *     parent_hash String, -- or FixedString(N)
 *     -- parent_number UInt64, -- should be present only in Solana datasets
 *     timestamp DateTime -- optional
 * )
 * ```
 *
 * It is used to store a list of completely processed blocks and is populated by the framework.
 *
 * All other members of the target database must be block item tables of the following structure:
 *
 * ```sql
 * CREATE TABLE {block_item_name}
 * (
 *     block_number UInt64, -- or UInt32
 *     block_hash String, -- or FixedString(N), this field is optional
 *     block_timestamp DateTime -- this field is optional
 *     -- other fields
 * )
 * ```
 *
 * `block_number`, `block_hash` and `block_timestamp` fields are populated by the framework.
 * Rows themselves and their other fields come from a mapping function.
 *
 * All tables are persisted independently.
 *
 * For each table there are low/high-watermark options
 * that regulate respectively the minimum number of rows to insert in a single query
 * and the maximum number of rows to buffer before holding back the data production.
 *
 * Insertions to the `blocks` table are regulated in a similar fashion;
 * however, watermark defaults are different:
 *
 *  * low watermark: `1024`
 *  * high watermark: `4096`
 *
 * The framework produces rows for the 'blocks' table
 * as data related to a given block becomes fully persisted.
 *
 * At the chain head low watermarks are ignored.
 * Once the process reaches the chain head,
 * all buffered data is always flushed and fully persisted.
 *
 * At the start of processing all rows related to partially written blocks
 * are deleted with the `DELETE` statement.
 *
 * Similarly, in the case of a chain fork, the `DELETE` statement is used to delete
 * all the rows related to roll-backed blocks.
 */
export function runClickhouseProcessing<B extends BlockBase, R extends {[P in keyof R]: object[]}>(args: ProcessorArgs<B, R>): void {
    runProgram(async () => {
        let clickhouse = new ClickhouseClient(args.clickhouse)

        let tableList = await inspectDatabase(clickhouse, args.clickhouseDatabase)

        log.debug({tableList}, 'database inspection finished')

        let head = await clickhouse.query<BlockRef>(
            `SELECT number, hash FROM ${args.clickhouseDatabase}.blocks ORDER BY number DESC LIMIT 1`
        ).then(res => {
            return maybeLast(res.data)
        })

        if (head) {
            log.debug({head}, 'processing head')
        }

        await clearPartialData(clickhouse, args.clickhouseDatabase, tableList, head)

        let tableMap: Record<string, TableOptions> = {}
        for (let table of tableList) {
            tableMap[table] = args.clickhouseTables?.[table] ?? {}
        }

        let writer = new BlockWriter(
            clickhouse,
            args.clickhouseDatabase,
            tableMap
        )

        let metrics = new Metrics()

        let {source, map} = args
        try {
            for await (let batch of source.createDataStream(head)) {
                let nRows = 0

                for (let block of batch.blocks) {
                    let tables = await map(block)
                    await writer.drain()
                    writer.push({
                        header: block.header,
                        tables
                    })

                    // track the number of inserted rows for performance stats
                    nRows += 1
                    for (let table in tables) {
                        nRows += tables[table].length
                    }
                }

                if (batch.blocks.length == 0 || (batch.headNumber ?? -1) <= last(batch.blocks).header.number) {
                    await writer.flush()
                }

                metrics.registerBatch(batch, nRows)
            }
        } catch(err: any) {
            if (writer.isHealthy) {
                await writer.flush().catch(err => {
                    log.error(err, 'final flush of already mapped data failed')
                })
            }
            throw err
        }

        await writer.flush()

        metrics.report()
    }, err => {
        log.fatal(err)
    })
}


async function clearPartialData(
    clickhouse: ClickhouseClient,
    database: string,
    tableList: string[],
    head: BlockRef | undefined
): Promise<void>
{
    if (head) {
        for (let table of tableList) {
            await clickhouse.command(`DELETE FROM ${database}.${table} WHERE block_number > ${head.number}`)
            log.debug(`cleared partial data in '${table}'`)
        }
    } else {
        for (let table of tableList) {
            await clickhouse.command(`DELETE FROM ${database}.${table} WHERE block_number >= 0`)
            log.debug(`cleared '${table}'`)
        }
    }
}


async function inspectDatabase(clickhouse: ClickhouseClient, db: string): Promise<string[]> {
    let columns = await clickhouse.query<{table: string, name: string, type: string}>(
        'SELECT table, name, type FROM system.columns WHERE database = {db:String}',
        {db}
    ).then(res => {
        return groupBy(res.data, it => it.table)
    })

    interface TypeDef {
        match(type: string): boolean
        description: string
    }

    let BlockNumber: TypeDef = {
        description: 'only UInt32 and UInt64 types are allowed',
        match(type: string): boolean {
            return ['UInt32', 'UInt64'].includes(type)
        }
    }

    let Hash: TypeDef = {
        description: 'only String or FixedString(N) types are allowed',
        match(type: string): boolean {
            return type == 'String' || type.startsWith('FixedString')
        }
    }

    let DataTime: TypeDef = {
        description: 'only DateTime type is allowed',
        match(type: string): boolean {
            return type == 'DateTime'
        }
    }

    for (let [table, fields] of columns.entries()) {
        function assertColumn(name: string, type: TypeDef, optional?: boolean) {
            let def = fields.find(f => f.name === name)
            if (def == null) {
                if (optional) return
                throw new Error(`table '${db}.${table}' does not have column '${name}'`)
            }

            if (!type.match(def.type)) throw new Error(
                `column '${name}' of table '${db}.${table}' has unsupported type ${def.type}, ${type.description}`
            )
        }

        if (table == 'blocks') {
            assertColumn('number', BlockNumber)
            assertColumn('hash', Hash)
            assertColumn('parent_number', BlockNumber, true)
            assertColumn('parent_hash', Hash)
            assertColumn('timestamp', DataTime, true)
        } else {
            assertColumn('block_number', BlockNumber)
            assertColumn('block_hash', Hash)
            assertColumn('block_timestamp', DataTime, true)
        }
    }

    if (!columns.has('blocks')) throw new Error(`'blocks' table is not defined in database '${db}'`)

    return Array.from(columns.keys()).filter(table => table != 'blocks')
}


class Metrics {
    private lastBlock = -1
    private lastTick = process.hrtime.bigint()
    private lastReportTime = Number.NEGATIVE_INFINITY
    private blockSpeed = new Speed()
    private insertSpeed = new Speed()
    private reportTimeout = new Timer(5000, () => this.report())

    registerBatch(batch: {blocks: BlockBase[]}, rows: number): void {
        if (batch.blocks.length == 0) return

        let lastBlock = last(batch.blocks).header.number
        let now = process.hrtime.bigint()

        if (this.lastBlock < 0) {
            this.lastBlock = batch.blocks[0].header.number - 1
        }

        this.blockSpeed.push(Math.max(0, lastBlock - this.lastBlock), this.lastTick, now)
        this.insertSpeed.push(rows, this.lastTick, now)

        this.lastBlock = lastBlock
        this.lastTick = now

        this.reportUpdates()
    }

    getStatusLine(): string {
        return `last block: ${this.lastBlock}, ` +
            `rate: ${Math.round(this.blockSpeed.speed())} blocks/sec, ` +
            `${Math.round(this.insertSpeed.speed())} rows/sec`
    }

    report(): void {
        log.info(this.getStatusLine())
        this.lastReportTime = Date.now()
        this.reportTimeout.stop()
    }

    private reportUpdates(): void {
        let now = Date.now()
        let delay = 5000 - (now - this.lastReportTime)
        if (delay > 0) {
            this.reportTimeout.start(delay)
        } else {
            this.report()
        }
    }
}
