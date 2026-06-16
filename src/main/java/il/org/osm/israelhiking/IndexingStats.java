package il.org.osm.israelhiking;

import java.util.concurrent.atomic.LongAdder;

/**
 * Cohesive holder for the bulk-indexing accounting counters and the failure-threshold logic, kept
 * out of PlanetSearchProfile (whose focus is OSM-feature -> document conversion). The counters are
 * the lossless-indexing invariant: emitted == indexed + failed (+ transient). They are
 * mutated from two places — PlanetSearchProfile on every emit (the emitted buckets), and
 * AccountingBulkListener from the bulk callbacks (indexed/failed/transient) — so the LongAdders are
 * exposed for the listener to share.
 */
final class IndexingStats {

  // Documents confirmed indexed by Elasticsearch.
  final LongAdder indexedCount = new LongAdder();
  // Failures split by destination index: fail the build on points (missing search results), only
  // warn on bbox geo_shape rejects (degenerate OSM geometries that don't affect name search). The
  // sum preserves the lossless invariant emitted == indexed + failed.
  final LongAdder failedPointsCount = new LongAdder();
  final LongAdder failedBboxCount = new LongAdder();
  // Separate bucket for points ops charged only after the bounded retry/backoff in the whole-batch
  // (Throwable) path is exhausted. These transient charges (connection/socket timeouts, 429/5xx)
  // are likely phantom, tolerated generously, with the reconcile gate as the backstop. Kept
  // distinct so a real per-item 4xx mass break still trips the strict guard; transientBbox is the
  // warn-only analogue.
  final LongAdder transientPointsCharges = new LongAdder();
  final LongAdder transientBboxCharges = new LongAdder();
  // Emitted/attempted docs: once per bulkIngester.add(...). Lets us assert emitted == indexed +
  // failed after flush(), so a doc that never reached a batch is detectable.
  final LongAdder emittedCount = new LongAdder();
  // Emitted POINTS only (excludes bbox): the reconcile gate and points thresholds are denominated
  // in points.
  final LongAdder emittedPointsCount = new LongAdder();

  long getIndexedCount() {
    return indexedCount.sum();
  }

  /** Total failed documents across all indices (points + bbox). Keeps the
   *  lossless invariant emitted == indexed + failed. */
  long getFailedCount() {
    return failedPointsCount.sum() + failedBboxCount.sum();
  }

  /** Failed documents destined for the points index — these are missing SEARCH
   *  results and must break the build. */
  long getFailedPointsCount() {
    return failedPointsCount.sum();
  }

  /** Failed bbox documents (geo_shape rejects on degenerate OSM geometries) —
   *  warn-only; they don't affect name search. */
  long getFailedBboxCount() {
    return failedBboxCount.sum();
  }

  /** Points ops charged to the TRANSIENT bucket after retries were exhausted
   *  (client-side connection/socket timeouts, 429/5xx). Likely phantom; tolerated
   *  generously and backstopped by the reconcile gate. */
  long getTransientPointsCharges() {
    return transientPointsCharges.sum();
  }

  /** Bbox ops charged to the transient bucket (warn-only analogue). */
  long getTransientBboxCharges() {
    return transientBboxCharges.sum();
  }

  /** Total transient charges across both indices (points + bbox). */
  long getTransientCharges() {
    return transientPointsCharges.sum() + transientBboxCharges.sum();
  }

  long getEmittedCount() {
    return emittedCount.sum();
  }

  /** Emitted POINTS documents only (excludes bbox). Denominator for the points
   *  failure thresholds and the reconcile gate. */
  long getEmittedPointsCount() {
    return emittedPointsCount.sum();
  }

  // Two-bucket points guard, replacing an all-or-nothing failedPointsCount > 0 predicate. Two
  // independent thresholds apply to POINTS:
  //   - genuine per-item data failures (4xx) are missing search results, so the bar is strict.
  //   - transient whole-batch charges (after retries) are likely phantom, so tolerated generously.
  // A real mass break still fails loudly via the genuine bucket; the reconcile gate is the backstop.

  /** Strict threshold for genuine per-item points data failures. */
  static long genuineFailureThreshold(long emitted) {
    return Math.max(50L, (long) Math.ceil(emitted * 0.000001)); // 0.0001%
  }

  /** Generous threshold for transient points charges (likely-phantom timeouts). */
  static long transientChargeThreshold(long emitted) {
    return Math.max(5_000L, (long) Math.ceil(emitted * 0.0005)); // 0.05%
  }

  /**
   * True when this run lost too many POINTS documents to treat the index as complete: a strict bar
   * on genuine per-item failures and a far more tolerant bar on transient charges. bbox failures
   * are warn-only and never trip this. Pure predicate so the decision is unit-testable.
   */
  boolean hasIndexingFailures() {
    long emittedPoints = emittedPointsCount.sum();
    return failedPointsCount.sum() > genuineFailureThreshold(emittedPoints)
        || transientPointsCharges.sum() > transientChargeThreshold(emittedPoints);
  }
}
