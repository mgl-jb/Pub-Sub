# Subscription filter language

A subscription receives a message when any of its rules matches. A rule's condition is either a
**correlation filter** (exact matches, combined with AND) or a **SQL filter** (a boolean
expression). This document is the reference for the expression language.

The parser and evaluator live in `src/PubSub.Filters`. Expressions are parsed and evaluated
in process — they are **never** concatenated into a database query, so they carry no SQL
injection risk.

## Quick reference

```sql
sys.Subject = 'OrderPlaced' AND region IN ('emea', 'amer') AND total > 500
reference LIKE 'order-%' AND priority IS NOT NULL
EXISTS(customerTier) AND NOT (channel = 'internal')
price * quantity >= 1000
```

## Grammar

```ebnf
expression     = or_expression ;
or_expression  = and_expression , { "OR" , and_expression } ;
and_expression = not_expression , { "AND" , not_expression } ;
not_expression = [ "NOT" ] , comparison ;

comparison     = additive , [ comparison_tail ] ;
comparison_tail
               = ( "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=" ) , additive
               | "IS" , [ "NOT" ] , "NULL"
               | [ "NOT" ] , "LIKE" , string , [ "ESCAPE" , string ]
               | [ "NOT" ] , "IN" , "(" , additive , { "," , additive } , ")" ;

additive       = multiplicative , { ( "+" | "-" ) , multiplicative } ;
multiplicative = unary , { ( "*" | "/" | "%" ) , unary } ;
unary          = [ "-" | "+" ] , primary ;

primary        = literal
               | property
               | "EXISTS" , "(" , property , ")"
               | "(" , expression , ")" ;

property       = identifier | "sys" , "." , system_property | "[" , any_char , "]" ;
literal        = number | string | "TRUE" | "FALSE" | "NULL" ;
string         = "'" , { any_char | "''" } , "'" ;
```

Precedence, loosest binding first: `OR`, `AND`, `NOT`, comparison (including the `LIKE` / `IN` /
`IS` forms), additive, multiplicative, unary minus, primary.

## Properties

| Form | Reads |
| --- | --- |
| `region` | the application property `region` |
| `[order total]` | an application property whose name contains spaces or punctuation |
| `sys.Subject` | a built-in message property |

Keywords are **case-insensitive** (`and`, `AND`, `And` are the same). Property names are
**case-sensitive**, because application properties live in an ordinal dictionary — `region` and
`Region` are different properties.

### System properties

`sys.MessageId`, `sys.CorrelationId`, `sys.Subject`, `sys.ContentType`, `sys.SessionId`,
`sys.ReplyTo`, `sys.ReplyToSessionId`, `sys.To`, `sys.EnqueuedTime`, `sys.SequenceNumber`,
`sys.DeliveryCount`.

`sys.Label` and `sys.MessageType` are accepted as aliases for `sys.Subject`.

An unknown `sys.` name is **rejected when the rule is created**, not silently treated as absent.
A typo in a system property name would otherwise produce a subscription that quietly receives
nothing — a failure that only shows up in production, as missing messages.

## Three-valued logic

This is the part worth reading carefully. The language follows SQL, not C#: an expression
evaluates to TRUE, FALSE, or **UNKNOWN**, and **only TRUE routes a message**.

UNKNOWN arises when:

- a property is absent — `missing = 'x'` is UNKNOWN, not FALSE;
- either side of a comparison is null — including `value = NULL`;
- the two sides are not comparable — `'abc' > 5`;
- arithmetic is undefined — division by zero, overflow, `NaN`.

### Truth tables

| `AND` | TRUE | FALSE | UNKNOWN |
| --- | --- | --- | --- |
| **TRUE** | TRUE | FALSE | UNKNOWN |
| **FALSE** | FALSE | FALSE | **FALSE** |
| **UNKNOWN** | UNKNOWN | **FALSE** | UNKNOWN |

| `OR` | TRUE | FALSE | UNKNOWN |
| --- | --- | --- | --- |
| **TRUE** | TRUE | TRUE | **TRUE** |
| **FALSE** | TRUE | FALSE | UNKNOWN |
| **UNKNOWN** | **TRUE** | UNKNOWN | UNKNOWN |

`NOT UNKNOWN` is UNKNOWN.

The two bold cases are the ones people find surprising, and both are correct: no value of the
unknown operand could make `UNKNOWN AND FALSE` true, and none could make `UNKNOWN OR TRUE` false.

### Consequences to watch for

**`= NULL` never matches.** Not even when the value really is null. Use `IS NULL`:

```sql
value = NULL       -- UNKNOWN, always. Never routes.
value IS NULL      -- TRUE when the value is null.
```

**`NOT` does not turn a non-match into a match.** If `region` is absent, both `region = 'emea'`
and `NOT (region = 'emea')` are UNKNOWN, so neither routes the message. To catch absent
properties, ask about presence explicitly:

```sql
NOT EXISTS(region) OR region <> 'emea'
```

**`EXISTS` and `IS NOT NULL` are different questions.** `EXISTS(x)` asks whether the property is
present; `x IS NOT NULL` asks whether it carries a value. A property explicitly set to null is
present but null, and only `EXISTS` distinguishes that from absent.

**A failed comparison does not throw.** Comparing a string to a number yields UNKNOWN. A filter
runs against every message on its topic, including ones whose shape its author never anticipated;
throwing would turn one odd message into a failure for every subscriber.

## Operators

### Comparison

`=`, `<>` (or `!=`), `<`, `<=`, `>`, `>=`.

Numbers compare across CLR types — a property that arrives from JSON as `int`, `long`, `double`,
or `decimal` compares equal to the literal `100` in every case. Strings compare **ordinally**, so
`'EMEA' = 'emea'` is FALSE. Booleans compare by equality. Timestamps compare chronologically.

### `LIKE`

`%` matches any run of characters; `_` matches exactly one. The pattern is anchored to the whole
value, so `reference LIKE 'order'` does **not** match `preorder-1`.

Regex metacharacters are literal: `reference LIKE 'a.c'` matches the three characters `a.c`, not
`abc`. Use `ESCAPE` to match a literal wildcard:

```sql
reference LIKE '100!%' ESCAPE '!'   -- matches the literal string "100%"
```

`LIKE` against a non-string value is UNKNOWN.

### `IN`

```sql
region IN ('emea', 'amer')
total IN (baseValue * 2, 99)        -- list items may be expressions
```

A non-match is only definite once every candidate has been compared. `region IN ('emea', NULL)`
where `region` is `'apac'` is UNKNOWN, because the comparison against NULL was UNKNOWN — SQL
semantics again.

### Arithmetic

`+`, `-`, `*`, `/`, `%`. Integer operands stay integral (`1 + 2` is `3`, not `3.0`), except
division, which may produce a fraction (`7 / 2` is `3.5`). `+` also concatenates two strings.

Division by zero, overflow, and `NaN` all yield UNKNOWN rather than throwing.

## Rule actions

A rule may transform the copy of the message delivered to **its own subscription**, leaving the
stored message and every other subscription untouched.

```sql
SET priority = 'high'
SET lineTotal = price * quantity
SET isLarge = total >= 100
REMOVE internalTag
SET a = 1; SET b = 'two'; REMOVE c
```

Only application properties are writable. A rule cannot modify a system property — allowing that
would let one subscription corrupt the broker's own routing and delivery state.

Where several matching rules carry actions, only the first match's action is applied, so the
delivered message is a function of the rule set rather than of evaluation order.

## Limits

Rules are evaluated against every message on their topic, and where subscriptions can be created
by someone other than the operator they are attacker-influenced input. The parser therefore caps:

| Limit | Default |
| --- | --- |
| Expression length | 4096 characters |
| Nesting depth | 32 |
| `IN` list items | 128 |
| Identifier length | 128 characters |
| String literal length | 1024 characters |

Configure them with `FilterLimits`. The depth cap matters most: without it, a deeply nested
expression overflows the parser's stack, and a `StackOverflowException` terminates the process
rather than raising a catchable error.

Two further protections are structural rather than numeric. `LIKE` patterns are escaped before
translation to a regex, so a catastrophically backtracking pattern cannot be smuggled through, and
a 100 ms match timeout covers what escaping does not. Expressions never reach the database, so
injection-shaped input is either an ordinary string comparison or a parse error.

## Implementation notes

Compilation uses **closures** rather than `System.Linq.Expressions`: each AST node becomes a small
delegate closing over its children's delegates. Evaluation is then a chain of direct calls with no
per-message tree walking. This needs no runtime IL emission, so it works unchanged under Native
AOT and on runtimes that forbid dynamic code generation, at comparable speed. `LIKE` regexes and
`IN` candidate lists are built once at compile time for the same reason.

A rule is compiled once and evaluated constantly, so the cost model is "compile rarely, evaluate
per message". Compiled rules are immutable and safe to share across concurrent publishes.
