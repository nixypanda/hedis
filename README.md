# Hedis — a Redis Toy Clone

A **learning-focused Redis clone written in Haskell**, aimed at understanding Redis internals, protocol design, and replication mechanics rather than providing a production-ready datastore.

The name is **Hedis** (yes, not very creative 😄).

This project deliberately mirrors several real Redis design choices (single port for clients and replicas, RESP as the wire format, role-based execution) while keeping the codebase small enough to reason about.

> ⚠️ This is a toy implementation for study and experimentation. Correctness and clarity matter more than completeness or performance.

## Acknowledgements

This project the result of following the **Codecrafters Redis Challenge**.

Codecrafters provided the structure, constraints, and motivation to build a Redis-like system end-to-end, while still leaving enough freedom to explore design decisions deeply. Many features implemented here map directly to milestones from that challenge.

## Feature overview

Hedis implements the majority of features required by the Codecrafters Redis challenge, including:

- RESP protocol parsing and encoding
- Basic key-value commands
- Lists, streams, sorted sets, and geospatial commands
- Pub/Sub messaging
- Transactions
- Master–replica replication
- Replication handshake
- Replication offsets and acknowledgements
- `WAIT` command semantics

Goes without saying that this is _not_ a full Redis implementation, but it covers the essential surface area needed to understand how Redis behaves internally.

---

## Module layout

Below are only the **top-level modules**.

### app/

Entry points and startup wiring.

- CLI parsing and configuration
- Server startup and role selection (master / replica)

### Types/

**Protocol and core runtime types.**

Defines _what can exist_ in the system:

- Commands, results, pub/sub messages
- Replication and propagation types
- The `Redis` monad, environment, and capability typeclasses

### Resp/

**RESP wire format implementation.**

- RESP AST
- Parser and encoder

### Wire/

**Types ⇄ RESP translation layer.**

- Converts `Types.*` values to/from RESP
- Separate handling for client and replication traffic

**Why:** keeps wire compatibility concerns isolated from execution and storage logic.

### Dispatch/

**Command execution policy layer.**

- Authentication checks
- Transaction state enforcement
- Pub/Sub mode gating
- Routing commands to the correct store or subsystem

### Store/

**STM-backed data model.**

- In-memory stores for strings, lists, streams, sorted sets, etc.
- Isolated STM backends

### Server/

**Long-running IO loops.**

- Socket ownership
- Client vs replica demultiplexing
- Master and replica roles

### Other

**Self-contained subsystems.**

- `Geo`: geospatial commands
- `Rdb`: persistence and RDB parsing

---

## Developer experience

This project was primarily an experiment in seeing **how Haskell feels when pushed beyond contrived examples** into something stateful, concurrent, and protocol-heavy.

Some standout aspects of the experience:

- **STM was exceptional**. It eliminated explicit locking entirely, made concurrent updates composable, and allowed client commands, replication, pub/sub, and transactions to coexist safely. I genuinely cannot imagine how much harder this would have been in Python or Go, or how much time I would have spent fighting the compiler and borrow checker in Rust.
- Transactional logic composed naturally — complex operations reduced to straightforward `mapM_` blocks over STM actions, which felt almost absurdly powerful.
- The **type system caught an enormous amount of missing logic** early. This pushed the implementation beyond the bare Codecrafters requirements, especially around replication correctness.
- Clear **master / replica environment separation** at the type level eliminated entire classes of invalid commands at compile time.
- Adding new commands was mostly mechanical: add the constructor, let the compiler warn about non-exhaustive pattern matches, and fix each site.
- Where exhaustiveness wasn’t possible (notably RESP → command decoding), I introduced _dual conversion functions_. Any missed cases were reliably caught by Hedgehog property tests asserting round-trip behavior.
- Refactoring was phenomenal. I performed multiple large-scale refactors; recompiling was usually enough to surface everything that needed fixing. It honestly felt a bit magical.
- Parser combinators were a joy to use. Command parsing felt declarative and robust.
- RDB parsing with **Attoparsec** gave excellent low-level control without sacrificing clarity.
- Despite having very few explicit tests, confidence in the system remained high thanks to strong typing, total functions, and property tests where they mattered.
- The only consistently painful area was raw socket handling in the `IO` monad — thankfully a very small part of the codebase.

Overall, Haskell proved to be an outstanding fit for this kind of system: concurrency-heavy, protocol-driven, and correctness-oriented.

---

## Who this is for

This project is useful if you want to:

- Learn how Redis works internally
- Study replication protocols
- See a non-trivial STM-based system
- Explore real-world Haskell server design

If you want a Redis-compatible datastore — this is not it 🙂

---

**Suggested reading order:**

1. `Server.MasterLoop`
2. `Dispatch.Client`
3. `Types.Command`
4. `Wire.*`
5. `Store.*`
