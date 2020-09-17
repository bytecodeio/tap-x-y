# tap-x-y

## Streams

- sales_order_line
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- customer
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- inventory
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- invoice
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- inventory_movement
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- item
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
    - Transformation: objects de-nested, camel to snake case, `/` to underscore, remove `$` 

- stock_transfer
    - Primary keys: id
    - Replication strategy: Incremental (query filtered)
      - Bookmark query parameter: lastModified
      - Bookmark: lastModified
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
