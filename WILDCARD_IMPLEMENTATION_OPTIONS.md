# Wildcard Support for OpenSearch SQL/PPL Field Comparisons

## Objective
Enable wildcard pattern matching in OpenSearch SQL/PPL equality operators (`=` and `!=`) to support queries like:
- `firstname='A*'` - Match all firstnames starting with 'A'
- `account_number='1*'` - Match account numbers starting with '1' 
- `ip_field='192.168.*'` - Match IPs in the 192.168 subnet
- `age!='3*'` - Exclude ages in the 30s

The implementation should support OpenSearch native wildcards:
- `*` - matches zero or more characters
- `?` - matches exactly one character

## Design Options

### Option 1: SmartEquals Operator (Current Implementation)

**Description**: Single unified operator that handles all equality comparisons with automatic wildcard detection and type conversion.

**Implementation**:
- Single `SmartEquals` class implementing `FunctionImp2`
- Detects wildcards in string literals at runtime
- Automatically converts fields to strings when wildcards are present
- Handles mixed-type comparisons (e.g., `age='32'`)

**Pros**:
- Clean, unified implementation - one operator handles everything
- Transparent to users - no special syntax needed
- Supports all data types automatically
- Smart type coercion for both wildcard and exact matches
- Minimal code changes required

**Cons**:
- May have unexpected behavior (e.g., `id='123'` might match differently than `id=123`)
- Performance overhead from runtime wildcard detection
- Potential ambiguity in mixed-type comparisons
- Not working in current integration tests (function resolution issue)

### Option 2: Multiple Specialized Operators

**Description**: Separate wildcard-aware operators for different data types (WildcardAwareEqualsFunc, WildcardAwareIpEquals, etc.)

**Implementation**:
- Multiple classes, each handling specific type combinations
- Type-specific wildcard handling logic
- Registered with specific type signatures

**Pros**:
- Type-safe implementation
- Can optimize for specific data types
- Clear separation of concerns
- More predictable behavior

**Cons**:
- Code duplication across multiple classes
- Complex registration and resolution logic
- Harder to maintain and extend
- Still requires mixed-type support for numeric fields with wildcards

### Option 3: Explicit LIKE/ILIKE Operator

**Description**: Require users to explicitly use LIKE/ILIKE for pattern matching instead of overloading equals.

**Example Syntax**:
```sql
firstname LIKE 'A%'
CAST(account_number AS STRING) LIKE '1%'
```

**Pros**:
- Clear, unambiguous syntax
- Follows SQL standard conventions
- No surprises for users
- Likely already works without changes

**Cons**:
- Requires users to know SQL LIKE syntax (`%` and `_` instead of `*` and `?`)
- Requires explicit casting for non-string fields
- Less intuitive for OpenSearch users familiar with native wildcard syntax
- More verbose queries

### Option 4: New Wildcard Operator

**Description**: Introduce a new operator specifically for wildcard matching (e.g., `=~` or `MATCHES`).

**Example Syntax**:
```sql
firstname =~ 'A*'
account_number MATCHES '1*'
```

**Pros**:
- Explicit wildcard intent
- No ambiguity with standard equality
- Can use OpenSearch native wildcard syntax
- Clear distinction between exact and pattern matching

**Cons**:
- Requires learning new syntax
- Not standard SQL
- Requires parser changes
- May not be intuitive for new users

### Option 5: Query Rewriting Approach

**Description**: Rewrite queries at parse time to convert wildcard equals to appropriate operations.

**Implementation**:
- Detect wildcards during query parsing
- Rewrite `field='pattern*'` to appropriate LIKE or wildcard query
- Transform query before execution

**Pros**:
- Works with existing execution engine
- No changes to operator implementations
- Can optimize based on field types
- Transparent to execution layer

**Cons**:
- Complex rewriting logic
- Requires parser modifications
- May miss runtime-determined patterns
- Harder to debug and trace

## Current Issues

The main challenge with all approaches is that the custom operators aren't being invoked during query execution. The integration tests show:

1. **Type Resolution Failure**: The system rejects mixed-type comparisons like `[LONG,STRING]`
2. **Function Selection**: Custom functions aren't being selected over default implementations
3. **Registration Order**: May be issues with how functions are registered and resolved

## Recommendation

**Short term**: Fix the current SmartEquals implementation by:
1. Investigating why custom functions aren't being invoked
2. Checking if there's a different code path for PPL execution
3. Ensuring proper function registration and resolution

**Long term**: Consider Option 3 (Explicit LIKE) or Option 4 (New Operator) for clearer semantics and better maintainability. The explicit approach avoids surprises and makes wildcard intent clear.

## Implementation Status

- ✅ SmartEquals and SmartNotEquals classes created
- ✅ Type checkers support mixed-type comparisons  
- ✅ Wildcard detection and conversion logic implemented
- ✅ Functions registered in PPLFuncImpTable
- ❌ Integration tests failing - functions not being invoked
- ❌ Need to investigate function resolution mechanism