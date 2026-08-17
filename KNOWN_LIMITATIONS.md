# Known Limitations — iteration boundaries

Each iteration is a standalone rebuild at increasing sophistication (see
`README.md`). This tracks concrete cases where one iteration's approach
actually breaks or falls short in practice — not speculative gaps — and which
later iteration is expected to close it. New entries get added as they're
found; nothing here is pre-emptively guessed.

## Iteration 1 → Iteration 2

- **Hard zero-tolerance DQ asserts crash the whole run instead of reporting
  and continuing.** `assert_data_quality`'s dropoff-before-pickup check is
  correct to be zero-tolerance in principle — these rows are structurally
  invalid — but it's a plain Python `assert`, so the first violation raises
  and kills the entire script rather than flagging/excluding just the bad
  rows. Surfaced running `iteration1_ingest_clean.py --no-sample-data`
  against the real dataset for the first time (via the new `--sample-data`
  flag): 2 of ~4.95M `trips_valid` rows have `dropoff < pickup`. The ~5k-row
  sample fixture never happened to contain a row like this, so the gap went
  undetected until real data was actually run through it.
  **Expected fix:** Iteration 2's reusable DQ framework (schema/null-rate/
  uniqueness checks emitting a JSON/HTML report per `PROJECT_IDEAS.md`)
  should treat this as a captured, quantified finding — exclude the
  offending rows from the valid population and report them — rather than a
  process-killing assert.
