# Redpanda Connect Documentation

Comprehensive reference documentation for Redpanda Connect (formerly Benthos) used in CDC pipeline implementations.

## Quick Navigation

| Document | Description | Use When |
|----------|-------------|----------|
| [01-BLOBLANG-FUNDAMENTALS](01-BLOBLANG-FUNDAMENTALS.md) | Core Bloblang concepts | Learning basics, understanding syntax |
| [02-BLOBLANG-METHODS](02-BLOBLANG-METHODS.md) | Method reference | Looking up specific methods |
| [03-BLOBLANG-FUNCTIONS](03-BLOBLANG-FUNCTIONS.md) | Built-in functions | Using generators, environment, metadata |
| [04-HTTP-INPUTS](04-HTTP-INPUTS.md) | HTTP webhooks & polling | Building webhook receivers, API polling |
| [05-SQL-PATTERNS](05-SQL-PATTERNS.md) | SQL/PostgreSQL patterns | Database integrations |
| [06-ERROR-HANDLING](06-ERROR-HANDLING.md) | Error handling patterns | Building resilient pipelines |
| [07-PIPELINE-PATTERNS](07-PIPELINE-PATTERNS.md) | Complete pipeline examples | Reference implementations |
| [08-PIPELINE-TEMPLATING](08-PIPELINE-TEMPLATING.md) | **Templating system** | **Template structure, .blobl files, generation flow** |

---

## Purpose

This documentation supports future pipeline template implementations:

1. **PostgreSQL CDC Source** - Capturing changes from PostgreSQL databases
2. **HTTP Webhook Receiver** - Receiving webhooks with signature validation
3. **API Polling Pipelines** - Polling external APIs for changes
4. **Custom Transformations** - Building complex data transformations

---

## Key Topics Covered

### Bloblang Mapping Language
- Assignment (`root`, `this`)
- Variables (`let`, `$`)
- Conditionals (`if/else`, `match`)
- Error handling (`catch`, `throw`)
- Type coercion and manipulation

### HTTP Inputs
- Webhook receivers (`http_server`)
- API polling (`http_client`)
- JWT/signature validation
- OAuth 2.0 authentication
- Rate limiting and TLS

### SQL Integration
- PostgreSQL patterns
- Connection pooling
- Batching for performance
- UPSERT operations
- JSONB handling

### Error Handling
- Try/catch processors
- Dead letter queues
- Retry strategies
- Error routing

---

## Official Documentation

- [Redpanda Connect Home](https://docs.redpanda.com/redpanda-connect/home/)
- [Bloblang Guide](https://docs.redpanda.com/redpanda-connect/guides/bloblang/about/)
- [Components Catalog](https://docs.redpanda.com/redpanda-connect/components/about/)
- [Error Handling](https://docs.redpanda.com/redpanda-connect/configuration/error_handling/)
