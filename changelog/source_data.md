
## 2024-11-04:

## Tables Added:
- new_schema.json
## Tables Deleted:
- history_trades_schema.json
## Schema Changes:
|       Table Name                | Operation     | Columns                  |
|---------------------------------|---------------|--------------------------|
| dimMarkets_schema.json            | column_added    | counter_issuers                                    |
| dimMarkets_schema.json            | column_removed  | counter_issuer                                     |
| euro_ohlc_schema.json             | type_changed    | open (FLOAT -> INTEGER)                            |
| history_contract_events_schema.json | column_removed  | successful                                         |


## 2024-01-01

## Tables Added:
['new_table']

### Tables Removed:
['old_table']

### Schema Changes:
|       Table Name                | Operation     | Columns                  |
|---------------------------------|---------------|--------------------------|
| euro_ohlc                         | type_changed  | high (TIMESTAMP -> FLOAT) |
| account_signers                   | column_added  | ledger_sequences         |
