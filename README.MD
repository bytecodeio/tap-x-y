# tap-x-y

## Streams

- sales_order_line
    - Primary keys: order, item
    - Replication strategy: Incremental (query filtered)
      - Date Windows: Lookback window for capturing recent changes to data
      - Bookmark query parameter: orderDate
      - Bookmark: orderDate
    - Transformation: objects de-nested, camel to snake case, `/` to underscore

- customer
    - Primary keys:  email
    - Replication strategy: Incremental (query filtered)
      - Date Windows: Lookback window for capturing recent changes to data
      - Bookmark query parameter: lastTxnDate
      - Bookmark: lastTxnDate
    - Transformation: objects de-nested, camel to snake case, `/` to underscore

- inventory
    - Primary keys: item
    - Replication strategy: Full table
    - Transformation: objects de-nested, camel to snake case, `/` to underscore

- invoice
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Date Windows: Lookback windows for capturing recent changes to data
      - Bookmark query parameter: orderDate
      - Bookmark: orderDate
    - Transformation: objects de-nested, camel to snake case, `/` to underscore

- inventory_movement
    - Primary keys: item, record_type, date
    - Replication strategy: Incremental (query filtered)
      - Date Windows: Lookback windows for capturing recent changes to data
      - Bookmark query parameter: date
      - Bookmark: date
    - Transformation: objects de-nested, camel to snake case, `/` to underscore

- item
    - Primary keys: id
    - Replication strategy: FULL_TABLE'
    - Transformation: objects de-nested, camel to snake case, `/` to underscore

- stock_transfer
    - Primary keys: item, stocktransfer
    - Replication strategy: Incremental (query filtered)
      - Date Windows: Lookback windows for capturing recent changes to data
      - Bookmark query parameter: date
      - Bookmark: date
    - Transformation: objects de-nested, camel to snake case, `/` to underscore

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