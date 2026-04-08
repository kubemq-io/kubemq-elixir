# Stress Test Boundary Map — Elixir SDK
Date: 2026-04-03 22:04
Duration per test: 2m | Broker: localhost:50000

## Phase 1 — Single Channel Max Rate

| Pattern | Max Rate | Fail Rate | Failure Reason |
|---------|----------|-----------|----------------|
| events | 5000/s | — | ceiling not found |
| events_store | 5000/s | — | ceiling not found |
| queue_stream | 3870/s | 4250/s | FAIL |
| queue_simple | 5000/s | — | ceiling not found |
| commands | 5000/s | — | ceiling not found |
| queries | 5000/s | — | ceiling not found |

## Phase 2 — Max Channels at Sustainable Rate

| Pattern | Rate/ch | Max Ch | Total msg/s |
|---------|---------|--------|-------------|
| events | 3500 | 10 | 35000 |
| events_store | 3500 | 4 | 14000 |
| queue_stream | 2709 | 1 | 2709 |
| queue_simple | 3500 | 7 | 24500 |
| commands | 3500 | 3 | 10500 |
| queries | 3500 | 3 | 10500 |

## Phase 3 — Multi-Pattern Combinations

| Patterns | Total msg/s | Verdict |
|----------|-------------|---------|
| Pub/Sub pair | 49000 | PASS |
| Queue pair | 27209 | PASS |
| RPC pair | 21000 | PASS |
| 3 patterns | 51709 | PASS |
| 4 patterns | 76209 | PASS |
| All 6 | 97209 | PASS |

## Summary

- **Peak single-pattern**: events @ 5000/s × 1ch
- **Peak multi-channel**: events @ 3500/s × 10ch = 35000/s
- **All 6 aggregate**: 97209/s
- **Bottleneck**: queue_stream (3870/s)
