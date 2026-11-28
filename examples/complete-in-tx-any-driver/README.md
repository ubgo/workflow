# complete-in-tx-any-driver

A step handler written against `workflow.Client` completes its step together with the app's own write through `workflow.TxCompleter`: on sqlstore the write and the step's success share one database transaction (`workflow.CompletionTx[*sql.Tx]`), on the memory driver used here the success is recorded only when the write returned nil. The declined invoice records nothing, and the handler's own result stands.

```sh
cd examples
go run ./complete-in-tx-any-driver
```

```text
supports CompleteInTx: true
invoice-42: output "invoice-42", row written: true
invoice-declined: output "not invoiced", row written: false
```

The guide is [Complete inside your transaction › Through the core interface, on any driver](../../docs/reliability/complete-in-transaction.md#through-the-core-interface-on-any-driver).
