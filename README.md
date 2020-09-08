# tap-x-y

## Replication

A number of endpoints do not expose a unique ID or a convenient 

## Streams

- sales_order_line
    - Primary keys: __record_guid (Generated UUID per extraction execution)
    - Replication strategy: Incremental (query filtered)
      - Date Windows: Lookback window for capturing recent changes to data
      - Bookmark query parameter: orderDate
      - Bookmark: orderDate
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- customer
    - Primary keys:  __record_guid (Generated UUID per extraction execution)
    - Replication strategy: Incremental (query filtered)
      - Date Windows: Lookback window for capturing recent changes to data
      - Bookmark query parameter: lastTxnDate
      - Bookmark: lastTxnDate
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- inventory
    - Primary keys: item, store
    - Replication strategy: Full table
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- invoice
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Date Windows: Lookback windows for capturing recent changes to data
      - Bookmark query parameter: orderDate
      - Bookmark: orderDate
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- inventory_movement
    - Primary keys: __record_guid (Generated UUID per extraction execution)
    - Replication strategy: Incremental (query filtered)
      - Date Windows: Lookback windows for capturing recent changes to data
      - Bookmark query parameter: date
      - Bookmark: date
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- item
    - Primary keys: id
    - Replication strategy: FULL_TABLE'
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- stock_transfer
    - Primary keys: __record_guid (Generated UUID per extraction execution)
    - Replication strategy: Incremental (query filtered)
      - Date Windows: Lookback windows for capturing recent changes to data
      - Bookmark query parameter: date
      - Bookmark: date
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

### Denesting and Transforms

Denesting promotes nested object properties to root-level. All keys converted from camel to snake case. For keys containing a `/` character, converted to "_"

Example orginal API reponse for Sales Order Line:

```json
    "charges/netAmountBeforeTax": {
        "amount": 6500,
        "currency": "USD"
    },
```

After denesting and transforms:

```json
    "charges_net_amount_before_tax_amount": 6500,
    "charges_net_amount_before_tax_currency": "USD"
```



---

Copyright &copy; 2020 Stitch
