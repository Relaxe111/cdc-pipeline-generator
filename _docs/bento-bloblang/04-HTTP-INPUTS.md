# HTTP Inputs for Webhooks

Redpanda Connect provides robust HTTP input capabilities for receiving webhooks and polling APIs.

---

## HTTP Server Input (Webhook Receiver)

The `http_server` input creates an HTTP endpoint to receive webhook requests.

### Basic Configuration

```yaml
input:
  http_server:
    path: /webhook
    allowed_verbs:
      - POST
      - PUT
    timeout: 10s
```

### Path Parameters

Capture dynamic path segments:

```yaml
input:
  http_server:
    path: /webhooks/{provider}/{event_type}
    allowed_verbs:
      - POST

pipeline:
  processors:
    - mapping: |
        # Access path parameters via metadata
        root = this
        root.provider = @http_server_path_params.provider
        root.event_type = @http_server_path_params.event_type
```

### Available Metadata

The `http_server` input adds these metadata fields:

| Metadata Field | Description |
|----------------|-------------|
| `http_server_verb` | HTTP method (GET, POST, etc.) |
| `http_server_request_path` | Full request path |
| `http_server_remote_addr` | Client IP address |
| `http_server_user_agent` | User-Agent header |
| `http_server_path_params` | Object with path parameter values |

**Accessing headers and query params:**

```yaml
pipeline:
  processors:
    - mapping: |
        # Access specific header
        root.content_type = @http_content_type
        root.auth_header = @Authorization
        
        # Access query parameters
        root.query_param = @query_param_name
```

### Rate Limiting

```yaml
input:
  http_server:
    path: /webhook
    rate_limit: my_rate_limit
    
rate_limit_resources:
  - label: my_rate_limit
    local:
      count: 100
      interval: 1s
```

### TLS Configuration

```yaml
input:
  http_server:
    path: /webhook
    tls:
      enabled: true
      cert_file: /path/to/cert.pem
      key_file: /path/to/key.pem
      # Optional: Client certificate verification (mTLS)
      client_certs:
        - cert_file: /path/to/client_ca.pem
```

### Sync Responses

Send synchronous responses back to webhook callers:

```yaml
input:
  http_server:
    path: /webhook
    sync_response:
      status: "${! if errored() { 500 } else { 200 } }"
      
output:
  sync_response: {}
```

### CORS Configuration

```yaml
input:
  http_server:
    path: /webhook
    cors:
      enabled: true
      allowed_origins:
        - "https://example.com"
        - "https://*.myapp.com"
```

---

## Webhook Signature Validation

### HMAC Signature Validation (Common Pattern)

Many webhooks use HMAC signatures. Here's how to validate them:

```yaml
input:
  http_server:
    path: /webhooks/{provider}

pipeline:
  processors:
    - mapping: |
        # Get the raw body and signature header
        let body = content()
        let signature = @X-Signature
        let secret = env("WEBHOOK_SECRET")
        
        # Compute expected signature
        let expected = $body.hash("hmac_sha256", $secret).encode("hex")
        
        # Validate
        root = if $signature != $expected {
            throw("Invalid webhook signature")
        } else {
            this
        }
```

### GitHub Webhook Validation

```yaml
pipeline:
  processors:
    - mapping: |
        let body = content()
        let signature = @X-Hub-Signature-256
        let secret = env("GITHUB_WEBHOOK_SECRET")
        
        # GitHub uses "sha256=" prefix
        let expected = "sha256=" + $body.hash("hmac_sha256", $secret).encode("hex")
        
        root = if $signature != $expected {
            throw("Invalid GitHub signature")
        } else {
            this
        }
        
        root.event = @X-GitHub-Event
        root.delivery_id = @X-GitHub-Delivery
```

### Stripe Webhook Validation

```yaml
pipeline:
  processors:
    - mapping: |
        let body = content()
        let sig_header = @Stripe-Signature
        let secret = env("STRIPE_WEBHOOK_SECRET")
        
        # Parse Stripe signature header (t=timestamp,v1=signature)
        let sig_parts = $sig_header.split(",").fold({}, part -> 
            let kv = part.split("=")
            tally.assign({kv.index(0): kv.index(1)})
        )
        
        let timestamp = $sig_parts.t
        let signature = $sig_parts.v1
        
        # Stripe signs: timestamp + "." + body
        let signed_payload = $timestamp + "." + $body
        let expected = $signed_payload.hash("hmac_sha256", $secret).encode("hex")
        
        root = if $signature != $expected {
            throw("Invalid Stripe signature")
        } else {
            this.parse_json()
        }
```

---

## JWT Token Validation

### Bearer Token Validation

```yaml
pipeline:
  processors:
    - mapping: |
        # Extract token from Authorization header
        let auth_header = @Authorization | ""
        let token = $auth_header.trim_prefix("Bearer ").trim()
        
        # Validate token
        root = if $token == "" {
            throw("Missing Authorization header")
        } else {
            this
        }
        
        # Parse JWT (choose algorithm based on your setup)
        let secret = env("JWT_SECRET")
        let claims = $token.parse_jwt_hs256($secret).catch(null)
        
        root = if $claims == null {
            throw("Invalid JWT token")
        } else if $claims.exp < now().ts_unix() {
            throw("Token expired")
        } else {
            this
            root._auth.user_id = $claims.sub
            root._auth.scopes = $claims.scopes | []
        }
```

### RS256 JWT Validation (Asymmetric)

```yaml
pipeline:
  processors:
    - mapping: |
        let token = @Authorization.trim_prefix("Bearer ")
        let public_key = file("/keys/public.pem")
        
        let claims = $token.parse_jwt_rs256($public_key).catch(null)
        
        root = if $claims == null {
            throw("Invalid JWT token")
        } else {
            this
            root.user = $claims.sub
        }
```

### Multiple Algorithm Support

```yaml
pipeline:
  processors:
    - mapping: |
        let token = @Authorization.trim_prefix("Bearer ")
        
        # Try different algorithms
        let hs256_secret = env("JWT_HS256_SECRET")
        let rs256_key = file("/keys/rs256_public.pem")
        
        let claims = $token.parse_jwt_hs256($hs256_secret)
            .catch($token.parse_jwt_rs256($rs256_key))
            .catch(null)
        
        root = if $claims == null {
            throw("Could not validate token with any algorithm")
        } else {
            this
            root.claims = $claims
        }
```

---

## HTTP Client Input (API Polling)

The `http_client` input polls HTTP APIs:

### Basic Polling

```yaml
input:
  http_client:
    url: https://api.example.com/data
    verb: GET
    rate_limit: polling_limit
    
rate_limit_resources:
  - label: polling_limit
    local:
      count: 1
      interval: 60s  # Poll once per minute
```

### Authentication Options

#### Basic Auth

```yaml
input:
  http_client:
    url: https://api.example.com/data
    basic_auth:
      enabled: true
      username: ${BASIC_AUTH_USER}
      password: ${BASIC_AUTH_PASS}
```

#### OAuth 2.0

```yaml
input:
  http_client:
    url: https://api.example.com/data
    oauth2:
      enabled: true
      client_id: ${OAUTH_CLIENT_ID}
      client_secret: ${OAUTH_CLIENT_SECRET}
      token_url: https://auth.example.com/oauth/token
      scopes:
        - read:data
```

#### JWT Auth

```yaml
input:
  http_client:
    url: https://api.example.com/data
    jwt:
      enabled: true
      private_key_file: /path/to/private_key.pem
      claims:
        iss: my-service
        aud: api.example.com
```

### Pagination

```yaml
input:
  http_client:
    url: https://api.example.com/items?page=${!counter()}
    verb: GET
    
pipeline:
  processors:
    # Stop when empty response
    - mapping: |
        root = if this.items.length() == 0 {
            deleted()
        } else {
            this.items
        }
    # Explode array into individual messages
    - unarchive:
        format: json_array
```

### Retry Configuration

```yaml
input:
  http_client:
    url: https://api.example.com/data
    retries: 3
    retry_period: 1s
    backoff_on:
      - 429  # Rate limited
      - 503  # Service unavailable
    drop_on:
      - 400  # Bad request - don't retry
      - 404  # Not found - don't retry
```

---

## Complete Webhook Example

Full webhook receiver with validation, routing, and error handling:

```yaml
input:
  http_server:
    path: /webhooks/{provider}
    allowed_verbs:
      - POST

pipeline:
  processors:
    # 1. Validate signature
    - mapping: |
        let provider = @http_server_path_params.provider
        let body = content()
        let signature = @X-Signature | @X-Hub-Signature-256 | ""
        
        let secrets = {
            "github": env("GITHUB_SECRET"),
            "stripe": env("STRIPE_SECRET"),
            "custom": env("CUSTOM_SECRET")
        }
        
        let secret = $secrets.get($provider) | ""
        
        root = if $secret == "" {
            throw("Unknown provider: " + $provider)
        } else {
            let expected = $body.hash("hmac_sha256", $secret).encode("hex")
            if !$signature.contains($expected) {
                throw("Invalid signature for " + $provider)
            } else {
                this
            }
        }
        
        root.provider = $provider
        root.received_at = now().ts_format("2006-01-02T15:04:05Z")
    
    # 2. Handle errors
    - catch:
        - mapping: |
            root = this
            root.error = error()
            root.error_type = "validation_failed"

output:
  switch:
    cases:
      # Route errors to DLQ
      - check: errored()
        output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: webhook-dlq
      
      # Route by provider
      - check: this.provider == "github"
        output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: github-events
            
      # Default route
      - output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: webhook-events
```

---

## See Also

- [01-BLOBLANG-FUNDAMENTALS.md](01-BLOBLANG-FUNDAMENTALS.md) - Core concepts
- [02-BLOBLANG-METHODS.md](02-BLOBLANG-METHODS.md) - JWT methods reference
- [06-ERROR-HANDLING.md](06-ERROR-HANDLING.md) - Error handling patterns
