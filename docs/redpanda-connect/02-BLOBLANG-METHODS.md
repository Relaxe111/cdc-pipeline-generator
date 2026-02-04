# Bloblang Methods Reference

Complete reference for Bloblang methods used in Redpanda Connect transformations.

---

## String Methods

### Basic Operations

```yaml
# Case conversion
root.upper = this.name.uppercase()          # "HELLO"
root.lower = this.name.lowercase()          # "hello"
root.cap = this.name.capitalize()           # "Hello world"

# Trimming
root.trimmed = this.text.trim()             # Remove leading/trailing whitespace
root.ltrimmed = this.text.trim_prefix(" ")  # Remove prefix
root.rtrimmed = this.text.trim_suffix("\n") # Remove suffix

# Length
root.len = this.text.length()               # Character count
```

### Searching & Matching

```yaml
# Contains checks
root.has_foo = this.text.contains("foo")
root.starts = this.text.has_prefix("http")
root.ends = this.text.has_suffix(".json")

# Find position
root.pos = this.text.index_of("needle")     # -1 if not found

# Splitting
root.parts = this.csv.split(",")            # Returns array
root.lines = this.text.split("\n")
```

### Modification

```yaml
# Replace
root.clean = this.text.replace_all("old", "new")
root.first = this.text.replace("old", "new", 1)  # Replace first N

# Slicing
root.substr = this.text.slice(0, 10)        # First 10 chars
root.rest = this.text.slice(5, -1)          # From index 5 to end

# Padding
root.padded = this.id.string().pad_left(10, "0")  # "0000000123"

# Repeat
root.sep = "-".repeat(20)                   # "--------------------"

# Escaping
root.escaped = this.html.escape_html()
root.unescaped = this.html.unescape_html()
root.url_safe = this.text.escape_url_query()
```

### Formatting

```yaml
# Format with placeholders
root.msg = "User %s has %d items".format(this.name, this.count)

# Quote/Unquote
root.quoted = this.text.quote()             # Add quotes
root.unquoted = this.text.unquote()         # Remove quotes
```

---

## Regex Methods

```yaml
# Test if matches
root.is_email = this.email.re_match("^[\\w.-]+@[\\w.-]+\\.[a-z]{2,}$")

# Find all matches
root.numbers = this.text.re_find_all("[0-9]+")

# Find with named groups
root.parsed = this.log.re_find_object(
    "(?P<level>\\w+): (?P<message>.*)"
)
# Result: {"level": "ERROR", "message": "something failed"}

# Replace with regex
root.clean = this.text.re_replace_all("[^a-zA-Z0-9]", "_")
```

---

## Number Methods

### Basic Math

```yaml
root.abs_val = this.number.abs()
root.ceil_val = this.number.ceil()
root.floor_val = this.number.floor()
root.round_val = this.number.round()

# Min/Max with value
root.clamped = this.value.max(0).min(100)   # Clamp between 0-100

# Power and log
root.squared = this.value.pow(2)
root.log_val = this.value.log()
root.log10 = this.value.log10()
```

### Integer Conversion

```yaml
root.int8 = this.value.int8()
root.int16 = this.value.int16()
root.int32 = this.value.int32()
root.int64 = this.value.int64()

root.uint8 = this.value.uint8()
root.uint16 = this.value.uint16()
root.uint32 = this.value.uint32()
root.uint64 = this.value.uint64()

root.float32 = this.value.float32()
root.float64 = this.value.float64()
```

---

## Timestamp Methods

### Parsing Timestamps

```yaml
# Parse from string (Go time format)
root.ts = this.date_str.ts_parse("2006-01-02T15:04:05Z07:00")
root.ts = this.date_str.ts_parse("2006-01-02")
root.ts = this.date_str.ts_parse("Jan 2, 2006 3:04 PM")

# Parse from Unix timestamp
root.ts = this.unix_ts.ts_unix()            # From seconds
root.ts = this.unix_ms.ts_unix_milli()      # From milliseconds
root.ts = this.unix_micro.ts_unix_micro()   # From microseconds
root.ts = this.unix_nano.ts_unix_nano()     # From nanoseconds
```

### Formatting Timestamps

```yaml
# Format to string
root.formatted = this.ts.ts_format("2006-01-02T15:04:05Z07:00")
root.date_only = this.ts.ts_format("2006-01-02")
root.human = this.ts.ts_format("Monday, January 2, 2006")

# Format to Unix
root.unix = this.ts.ts_unix()
root.unix_ms = this.ts.ts_unix_milli()
root.unix_nano = this.ts.ts_unix_nano()
```

### Timestamp Manipulation

```yaml
# Change timezone
root.utc = this.ts.ts_tz("UTC")
root.oslo = this.ts.ts_tz("Europe/Oslo")

# Round timestamp
root.rounded = this.ts.ts_round("1h")       # Round to nearest hour
root.rounded = this.ts.ts_round("15m")      # Round to 15 minutes

# Add/Subtract (ISO 8601 duration)
root.tomorrow = this.ts.ts_add_iso8601("P1D")
root.next_hour = this.ts.ts_add_iso8601("PT1H")
root.last_week = this.ts.ts_sub_iso8601("P7D")

# Extract components
root.year = this.ts.ts_format("2006").number()
root.month = this.ts.ts_format("01").number()
root.day = this.ts.ts_format("02").number()
```

**Go Time Format Reference:**

| Unit | Format |
|------|--------|
| Year | `2006` |
| Month | `01` or `Jan` |
| Day | `02` |
| Hour (24h) | `15` |
| Hour (12h) | `03` |
| Minute | `04` |
| Second | `05` |
| Timezone | `Z07:00` or `MST` |

---

## Array Methods

### Basic Operations

```yaml
root.len = this.items.length()
root.first = this.items.index(0)
root.last = this.items.index(-1)
root.slice = this.items.slice(0, 5)         # First 5 items
```

### Iteration

```yaml
# Map each element
root.names = this.users.map_each(user -> user.name)
root.doubled = this.numbers.map_each(n -> n * 2)

# Filter elements
root.active = this.users.filter(u -> u.active == true)
root.positive = this.numbers.filter(n -> n > 0)

# Check conditions
root.has_admin = this.users.any(u -> u.role == "admin")
root.all_valid = this.items.all(i -> i.valid == true)

# Find first matching
root.admin = this.users.find(u -> u.role == "admin")
```

### Aggregation

```yaml
root.total = this.prices.sum()
root.joined = this.names.join(", ")

# Fold/Reduce
root.total = this.items.fold(0, item -> tally + item.price)

# Unique values
root.unique = this.tags.unique()

# Sort
root.sorted = this.numbers.sort()
root.sorted = this.users.sort_by(u -> u.name)

# Flatten nested arrays
root.flat = this.nested.flatten()
```

### Modification

```yaml
root.with_new = this.items.append("new_item")
root.merged = this.arr1.merge(this.arr2)
root.concat = this.arr1.concat(this.arr2)
root.reversed = this.items.reverse()
```

### Enumeration

```yaml
# Add index to each element
root.indexed = this.items.enumerated()
# Result: [{"index": 0, "value": "a"}, {"index": 1, "value": "b"}]

# Zip two arrays
root.pairs = this.keys.zip(this.values)
# Result: [["key1", "val1"], ["key2", "val2"]]
```

---

## Object Methods

### Field Access

```yaml
root.keys = this.obj.keys()                 # Get all keys
root.values = this.obj.values()             # Get all values
root.pairs = this.obj.key_values()          # [{key: k, value: v}, ...]

# Dynamic field access
root.val = this.obj.get(this.field_name)

# Check if key exists
root.has_id = this.obj.exists("id")
```

### Modification

```yaml
# Add/update fields
root = this.obj.assign({"new_field": "value"})
root = this.obj.with("new_field", "value")

# Remove fields
root = this.obj.without("password", "secret")

# Merge objects
root = this.obj1.merge(this.obj2)

# Collapse nested structure
root = this.nested.collapse()
# {"a": {"b": 1}} -> {"a.b": 1}

# Explode flattened structure
root = this.flat.explode()
# {"a.b": 1} -> {"a": {"b": 1}}
```

### JSON Path

```yaml
# Query with JSON path
root.names = this.json_path("$.users[*].name")
root.first = this.json_path("$.items[0]")
```

---

## Parsing Methods

### JSON

```yaml
root = this.json_string.parse_json()
root.json_str = this.obj.format_json()
root.compact = this.obj.format_json_compact()
```

### YAML

```yaml
root = this.yaml_string.parse_yaml()
root.yaml_str = this.obj.format_yaml()
```

### XML

```yaml
root = this.xml_string.parse_xml()
root.xml_str = this.obj.format_xml()
```

### CSV

```yaml
# Parse CSV line
root = this.csv_line.parse_csv()

# Parse with headers
root = this.csv_data.parse_csv_to_objects()
```

### URL

```yaml
root = this.url_string.parse_url()
# Result: {scheme, host, path, query, fragment}
```

---

## Encoding Methods

### Base64

```yaml
root.encoded = this.text.encode("base64")
root.decoded = this.encoded.decode("base64")

# URL-safe base64
root.encoded = this.text.encode("base64url")
```

### Hex

```yaml
root.hex = this.bytes.encode("hex")
root.bytes = this.hex.decode("hex")
```

### Hashing

```yaml
root.md5 = this.text.hash("md5").encode("hex")
root.sha1 = this.text.hash("sha1").encode("hex")
root.sha256 = this.text.hash("sha256").encode("hex")
root.sha512 = this.text.hash("sha512").encode("hex")

# HMAC
root.hmac = this.text.hash("hmac_sha256", "secret_key").encode("hex")
```

### Encryption (AES)

```yaml
# Encrypt (returns bytes)
root.encrypted = this.plaintext.encrypt_aes("gcm", $key, $iv)

# Decrypt
root.decrypted = this.encrypted.decrypt_aes("gcm", $key, $iv)
```

---

## JWT Methods

### Parsing JWT

```yaml
# HS256/384/512 (symmetric)
root.claims = this.token.parse_jwt_hs256("secret")
root.claims = this.token.parse_jwt_hs384("secret")
root.claims = this.token.parse_jwt_hs512("secret")

# RS256/384/512 (asymmetric - public key)
root.claims = this.token.parse_jwt_rs256($public_key)
root.claims = this.token.parse_jwt_rs384($public_key)
root.claims = this.token.parse_jwt_rs512($public_key)

# ES256/384/512 (ECDSA)
root.claims = this.token.parse_jwt_es256($public_key)
root.claims = this.token.parse_jwt_es384($public_key)
root.claims = this.token.parse_jwt_es512($public_key)
```

### Signing JWT

```yaml
# Sign claims object
root.token = this.claims.sign_jwt_hs256("secret")
root.token = this.claims.sign_jwt_rs256($private_key)
```

### JWT Validation Pattern

```yaml
# Parse and validate
let claims = this.headers.Authorization
    .trim_prefix("Bearer ")
    .parse_jwt_hs256("secret")
    .catch(null)

root = if $claims == null {
    # Invalid token - reject
    throw("Invalid JWT token")
} else if $claims.exp < now().ts_unix() {
    # Token expired
    throw("Token expired")
} else {
    # Valid - continue processing
    this
    root.user_id = $claims.sub
}
```

---

## Password Verification

```yaml
# Verify against bcrypt hash
root.valid = this.password.compare_bcrypt(this.hash)

# Verify against argon2 hash
root.valid = this.password.compare_argon2(this.hash)
```

---

## See Also

- [01-BLOBLANG-FUNDAMENTALS.md](01-BLOBLANG-FUNDAMENTALS.md) - Core concepts
- [03-BLOBLANG-FUNCTIONS.md](03-BLOBLANG-FUNCTIONS.md) - Built-in functions
- [04-HTTP-INPUTS.md](04-HTTP-INPUTS.md) - HTTP webhook patterns
