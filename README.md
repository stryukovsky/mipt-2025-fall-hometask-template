# MIPT Blockchain Data Course Home Assignment

This repository serves as the starter template for the MIPT Blockchain Data course (Fall 2025) home assignment.

It is a multi-package pnpm workspace with the following structure:

* [core](./core) – ETL framework
* [evm](./evm) – example project that tracks ERC-20 transfers
* [solana](./solana) – example project that tracks Orca Exchange swaps on Solana
* [solution](./solution) – placeholder for the student’s solution
* [tasks](./tasks) – list of available assignments

## Prerequisites

* Node.js v22 or higher
* [pnpm v10](https://pnpm.io)
* Recent `docker(1)`
* MacOS or Linux

## Getting started

```bash
# Install dependencies
pnpm i

# Build the project
pnpm -r build

# Run one of the examples
cd evm
make up && sleep 3 && make db
node lib/main.js
```

## Next steps

1. Check out the inline docs for [`runClickhouseProcessing()`](./core/src/clickhouse-processor.ts).
2. Check out [evm/aggregation.md](./evm/aggregation.md), telling the story of how one can efficiently aggregate data on the ClickHouse side, avoiding the tricky business of doing that elsewhere in many practical cases.
3. The framework logs some info with [@subsquid/logger](https://github.com/subsquid/squid-sdk/tree/d4aec7e52c6cc0c915746b11683d6e73a975370a/util/logger). You can set environment variable `SQD_DEBUG=*` for the most verbose logging. More details about how to fine-tune log levels are available [here](https://github.com/subsquid/squid-sdk/tree/d4aec7e52c6cc0c915746b11683d6e73a975370a/util/logger#configuration).

## Known caveats

* Data processing may sometimes hang indefinitely if there are Subsquid Portal connection issues.
* ETL framework assumes some consistency guarantees from the ClickHouse database that might not hold for all ClickHouse setups by default. Although we have not analyzed the matter carefully, we believe it is possible to get required guarantees from all kinds of ClickHouse instances.
