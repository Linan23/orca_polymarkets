from dune_client.client import DuneClient
import pandas as pd

url = "https://api.dune.com/api/v1/query/2103719/results?api_key=dune_api_key"

dune = DuneClient(api_key="ydu7Kk9GvbLsf2lbskM6TFOHQM9erKxx")

'''
sql = """
SELECT 
    maker, 
    token_outcome_name,
    COUNT(*) as trade_count,
    SUM(shares) as total_shares,
    SUM(amount) as total_amount,
    AVG(price) as avg_price
FROM polymarket_polygon.market_trades
GROUP BY maker, token_outcome_name
HAVING SUM(shares) > 150
ORDER BY SUM(shares) DESC
LIMIT 10
"""

'''
sql = """
WITH buy_sell_actions AS (
    SELECT
        '0x' || SUBSTRING(LOWER(TRY_CAST(topic2 AS VARCHAR)), 27, 40) AS maker,
        '0x' || SUBSTRING(LOWER(TRY_CAST(topic3 AS VARCHAR)), 27, 40) AS taker,
        CASE
            WHEN makerAssetId = 0 THEN 'Buy'
            WHEN takerAssetId = 0 THEN 'Sell'
        END AS action,
        CASE
            WHEN makerAssetId = 0 THEN makerAmountFilled / 1e6       
            WHEN takerAssetId = 0 THEN takerAmountFilled / 1e6
        END AS amount_usdc,
        tx_hash,
        index as log_index
    FROM TABLE (
        decode_evm_event (
            abi => '{ "anonymous": false, "inputs": [ { "indexed": true, "internalType": "bytes32", "name": "orderHash", "type": "bytes32" }, { "indexed": true, "internalType": "address", "name": "maker", "type": "address" }, { "indexed": true, "internalType": "address", "name": "taker", "type": "address" }, { "indexed": false, "internalType": "uint256", "name": "makerAssetId", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "takerAssetId", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "makerAmountFilled", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "takerAmountFilled", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "fee", "type": "uint256" } ], "name": "OrderFilled", "type": "event" }',
            input => TABLE (
                SELECT *
                FROM polygon.logs
                WHERE topic0 = 0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6
                AND block_date >= DATE'2023-01-01'  -- Add date filter for performance
            )
        )
    )
)
SELECT
    mt.block_time,
    mt.block_number,
    mt.tx_hash,
    mt.maker,
    mt.taker,
    mt.token_outcome_name,
    mt.question,
    mt.price,
    mt.amount,
    mt.shares,
    mt.token_outcome,
    bsa.action AS maker_action,
    CASE 
        WHEN bsa.action = 'Buy' THEN 'Sell'
        WHEN bsa.action = 'Sell' THEN 'Buy'
    END AS taker_action
FROM polymarket_polygon.market_trades mt
LEFT JOIN buy_sell_actions bsa
    ON mt.tx_hash = bsa.tx_hash
    AND mt.maker = from_hex(substr(bsa.maker, 3))
WHERE mt.maker = 0xd7b0032c7288cc0832d278b6ec520eb00b0dbd69
ORDER BY mt.block_time DESC
LIMIT 50
"""

print("Running query on Dune (scanning polygon.logs + join; may take a long time)...")
results = dune.run_sql(query_sql=sql)
df = pd.DataFrame(results.get_rows())
df.to_csv("specific_whale_questions_all.csv", index=False)
print(f"Done. Saved {len(df)} rows to specific_whale_questions_all.csv")
