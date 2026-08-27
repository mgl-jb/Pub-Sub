# 4. Correlation filters plus a SQL-92-like expression language

Status: Accepted

## Context

Subscriptions need to select which of a topic's messages they receive. Filtering at the broker
rather than in the consumer means an unwanted message is never delivered at all, instead of being
delivered and discarded — a difference that compounds with fan-out.

## Decision

Two filter kinds. **Correlation filters** match exactly on system and application properties,
combined with AND. **SQL filters** evaluate a boolean expression: `region IN ('emea', 'amer') AND
total > 500`.

The expression language follows SQL's **three-valued logic**, not C# booleans. Comparing an absent
property yields UNKNOWN rather than false, and only TRUE routes a message.

## Consequences

The semantics match what Service Bus users expect, so existing rules transfer largely unchanged.

Three-valued logic is the part that surprises people, and it is deliberate. `value = NULL` is never
true, even when the value is null — `IS NULL` is the only definite test. `NOT (region = 'emea')`
does not match a message with no `region`. These are SQL's rules, and reproducing them faithfully
is better than inventing a subtly different set that fails differently.

Comparing mismatched types yields UNKNOWN instead of throwing. A filter runs against every message
on its topic, including ones whose shape its author never anticipated; throwing would turn one
oddly-shaped message into a failure for every subscriber.

Because rules are attacker-influenced wherever subscriptions are not operator-authored, the parser
caps expression length, nesting depth, and `IN` list size. The depth cap matters most: without it a
deeply nested expression overflows the parser's stack, and a `StackOverflowException` kills the
process rather than raising a catchable error.

Expressions are parsed and evaluated in process and never reach the database, so there is no SQL
injection surface. `LIKE` patterns are escaped before translation to a regex, so a catastrophically
backtracking pattern cannot be smuggled through.

## Alternatives considered

**Correlation filters only.** Covers most real routing, is index-friendly, and needs no parser.
Rejected as too limiting: threshold and range conditions are common enough to be worth the
expression engine.

**CloudEvents Subscriptions API filters.** Standards-aligned and composable. Less familiar to the
audience this is modelled for, and no more capable for the cases that matter here.

**Compiling with `System.Linq.Expressions`.** The obvious way to compile an AST. Closure
compilation was chosen instead — each node becomes a delegate closing over its children — because
it needs no runtime IL emission, so it works under Native AOT and on runtimes that forbid dynamic
code generation, at comparable speed.
