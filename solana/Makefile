up:
	@docker compose up -d


down:
	@docker compose down -v


db:
	@cat db.sql | docker run -i \
		--network="$(notdir $(shell pwd))_default" \
		--entrypoint clickhouse-client \
		clickhouse:lts-jammy \
		--host clickhouse \
		--password 123


# Generate data decoders from Anchor IDL files
#
# IDL file can be specified in three ways:
#
# 1. as a plain JSON file:
#
# squid-solana-typegen src/abi whirlpool.json
#
# 2. as a contract address (to fetch the IDL from RPC endpoint)
#
# squid-solana-typegen src/abi whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc
#
# In all cases typegen will use IDL's basename as a basename of generated files.
# You can overwrite basename using fragment (#) suffix as it is done below.
abi:
	@npx squid-solana-typegen ./src/abi whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc#whirlpool


.PHONY: up down db abi
