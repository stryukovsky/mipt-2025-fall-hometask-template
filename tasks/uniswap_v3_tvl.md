# Uniswap V3 pool TVL history

Develop ETL pipeline, that would allow to query TVL history data points from ClickHouse database.

**TVL** (Total Value Locked) of Uniswap liquidity pool is measured as a pair of values (`amount0`, `amount1`),
that represent the total amount of tokens (`token0` and `token1`) managed by the pool.

Note, that this value is different from *virtual liquidity* 
(as we are interested in total amount of tokens across the whole price range)
and does not include fees due to pool administrates or liquidity providers.

## Details

ETL pipeline should maintain a table (or an equivalent view) of the following structure:

| Column          | Type            | Comment                            |
|-----------------|-----------------|------------------------------------|
| block_number    | UInt32          |                                    |
| block_timestamp | DateTime        |                                    |
| pool            | FixedString(42) | Liquidity pool contract address    |
| token0          | FixedString(42) | Token 0 address                    |
| token1          | FixedString(42) | Token 1 address                    |
| amount0         | UInt256         | TVL of token 0 at the end of block |
| amount1         | UInt256         | TVL of token 1 at the end of block |

It should be possible to efficiently fetch the data for any given block or block range.

Table should be always up to date with the latest known block.

For this task you should collect the TVL for the following pools of Ethereum mainnet:

* [0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8](https://etherscan.io/address/0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8)
* [0x5777d92f208679DB4b9778590Fa3CAB3aC9e2168](https://etherscan.io/address/0x5777d92f208679DB4b9778590Fa3CAB3aC9e2168)

## References

* [Uniswap V3 white paper](https://app.uniswap.org/whitepaper-v3.pdf)
* [Uniswap V3 pool contract sources](https://github.com/Uniswap/v3-core/blob/main/contracts/UniswapV3Pool.sol)
