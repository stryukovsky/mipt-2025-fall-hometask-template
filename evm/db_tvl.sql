CREATE DATABASE u3_tvl_src;

CREATE TABLE u3_tvl_src.blocks (
    number UInt64,
    hash FixedString(66),
    parent_hash FixedString(66),
    timestamp DateTime
)
ENGINE = MergeTree()
ORDER BY number;


CREATE TABLE u3_tvl_src.liquidity (

  block_number UInt64,
  block_hash FixedString(66),
  block_timestamp DateTime,
  pool FixedString(42),
  amount Int128,
)
ENGINE = MergeTree()
ORDER BY block_number;

CREATE TABLE u3_tvl_src.price (

  block_number UInt64,
  block_hash FixedString(66),
  block_timestamp DateTime,
  pool FixedString(42),
  price FixedString(42),
)
ENGINE = MergeTree()
ORDER BY block_number;

CREATE VIEW u3_tvl_src.pool_token_amounts AS
SELECT
    p.block_number,
    p.block_hash,
    p.block_timestamp,
    p.pool,
    l.amount AS liquidity,
    p.price,
    toFloat64(p.price) AS price_numeric,
    if(toFloat64(p.price) > 0,
       toFloat64(l.amount) / sqrt(toFloat64(p.price)),
       0
    ) AS token_a_amount,
    if(toFloat64(p.price) > 0,
       toFloat64(l.amount) * sqrt(toFloat64(p.price)),
       0
    ) AS token_b_amount,
    l.block_number AS liquidity_block_number
FROM u3_tvl_src.price p
ASOF LEFT JOIN u3_tvl_src.liquidity l
    ON p.pool = l.pool AND p.block_number >= l.block_number;
