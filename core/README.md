## Package structure

### [clickhouse-processor](./src/clickhouse-processor.ts)

`runClickhouseProcessing()` implements the ETL processing loop
that transforms block data into table rows and persists them in the ClickHouse database.

### [clickhouse/client](./src/clickhouse/client.ts)

A small wrapper around ClickHouse HTTP API with the focus on minimal overhead data uploads.

### [portal/core](./src/portal/core)

* defines data types that describe the shape of a Portal query and the resulting data
* provides an elaborate data-streaming procedure implemented in a transport-agnostic manner

### [portal/data-source](./src/portal/data-source.ts)

`PortalDataSource` can be passed directly to `runClickhouseProcessing()` to feed it
with Portal data.
