package il.org.osm.israelhiking;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

import org.apache.http.conn.ConnectTimeoutException;
import org.elasticsearch.client.ResponseException;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

/**
 * BulkListener that surfaces and counts every bulk outcome instead of swallowing errors.
 * Extracted from an anonymous inner class so the per-item classification (indexed vs failed) and
 * the whole-batch failure path are unit-testable by driving afterBulk directly.
 */
final class AccountingBulkListener implements BulkListener<Void> {
  // Log under the profile's logger so the indexing diagnostics stay in one place (and tests that
  // assert on the PlanetSearchProfile logger still see them).
  private static final Logger LOGGER = Logger.getLogger(PlanetSearchProfile.class.getName());

  // Retry/backoff tuning for the whole-batch transient path.
  static final int MAX_RETRY_ATTEMPTS = 5;          // total resubmits (1s,2s,4s,8s,16s)
  static final long BASE_BACKOFF_MILLIS = 1_000L;   // first backoff
  static final long MAX_BACKOFF_MILLIS = 16_000L;   // cap per attempt

  static final int RETRY_POOL_SIZE = 4;
  // One batch's retries are bounded (~5 attempts x 16s backoff + ES socket timeouts ~= a few minutes);
  // 30 min leaves ample room for a queue of batches draining on RETRY_POOL_SIZE threads at shutdown.
  static final long RETRY_DRAIN_TIMEOUT_MINUTES = 30;

  /** Injectable sleep so the backoff is testable without real delays. */
  @FunctionalInterface
  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final ElasticsearchClient esClient;
  private final LongAdder indexedCount;
  private final LongAdder failedCount;
  private final Sleeper sleeper;
  private final ExecutorService retryExecutor;

  AccountingBulkListener(ElasticsearchClient esClient, IndexingStats stats) {
    this(esClient, stats.indexedCount, stats.failedCount, Thread::sleep, defaultRetryExecutor());
  }

  /** Full constructor; tests inject a no-op Sleeper and a same-thread executor for determinism. */
  AccountingBulkListener(ElasticsearchClient esClient, LongAdder indexedCount, LongAdder failedCount,
      Sleeper sleeper, ExecutorService retryExecutor) {
    this.esClient = esClient;
    this.indexedCount = indexedCount;
    this.failedCount = failedCount;
    this.sleeper = sleeper;
    this.retryExecutor = retryExecutor;
  }

  private static ExecutorService defaultRetryExecutor() {
    return Executors.newFixedThreadPool(RETRY_POOL_SIZE, r -> {
      Thread t = new Thread(r, "es-bulk-retry");
      t.setDaemon(true);
      return t;
    });
  }

  private void recordFailure() {
    failedCount.increment();
  }

  @Override
  public void beforeBulk(long executionId, BulkRequest request, List<Void> contexts) {
    // no-op
  }

  /**
   * Classify a (possibly 200-with-errors) bulk response. Fast-paths an all-success full-length
   * response; otherwise walks per op so successes are counted, per-item 4xx charged as failed, and
   * retryable items (429/5xx) or ops missing from a short response fed through the backoff path.
   */
  @Override
  public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, BulkResponse response) {
    if (!response.errors() && response.items().size() == request.operations().size()) {
      indexedCount.add(response.items().size());
      return;
    }
    List<BulkOperation> retryable = classifyAndCount(request.operations(), response);
    if (!retryable.isEmpty()) {
      LOGGER.warning(() -> "Bulk request " + executionId + " had " + retryable.size()
          + " retryable per-item failure(s); resubmitting with backoff.");
      offloadRetry(executionId, retryable);
    }
  }

  /** Whole-batch failure (no per-item breakdown): retry transient errors with backoff, else count failed. */
  @Override
  public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, Throwable failure) {
    if (isRetryable(failure)) {
      LOGGER.warning(() -> "Bulk request " + executionId + " failed transiently ("
          + describe(failure) + "); retrying with backoff.");
      offloadRetry(executionId, request.operations());
    } else {
      LOGGER.severe(() -> "Bulk request " + executionId + " failed non-retryably ("
          + describe(failure) + "); counting " + request.operations().size() + " op(s) as failed.");
      request.operations().forEach(op -> recordFailure());
    }
  }

  /** Run the retry off the ingester's scheduler thread so its backoff can't head-of-line-block ingest. */
  private void offloadRetry(long executionId, List<BulkOperation> operations) {
    List<BulkOperation> batch = new ArrayList<>(operations);
    retryExecutor.submit(() -> resubmitWithBackoff(executionId, batch));
  }

  /**
   * Block until every offloaded retry has finished. Must run after the BulkIngester is closed and
   * before the alias swap, or in-flight retries' docs would race the swap and be lost.
   */
  void awaitRetries() {
    retryExecutor.shutdown();
    try {
      if (!retryExecutor.awaitTermination(RETRY_DRAIN_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
        LOGGER.severe("Bulk retries did not drain within " + RETRY_DRAIN_TIMEOUT_MINUTES
            + " min; forcing shutdown — some re-indexed docs may not have landed before the alias swap.");
        retryExecutor.shutdownNow();
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      retryExecutor.shutdownNow();
    }
  }

  /**
   * Resubmit a timed-out batch via a fresh synchronous bulk call, with exponential backoff +
   * jitter, up to MAX_RETRY_ATTEMPTS times. Each attempt's response is reconciled per item:
   * successes counted as indexed, per-item 4xx charged as genuine failures (never retried), and
   * only retryable ops carry over. Whatever remains after the final attempt is counted as failed.
   */
  private void resubmitWithBackoff(long executionId, List<BulkOperation> operations) {
    List<BulkOperation> pending = new ArrayList<>(operations);
    for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS && !pending.isEmpty(); attempt++) {
      long backoff = backoffMillis(attempt);
      try {
        sleeper.sleep(backoff);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        LOGGER.warning("Retry interrupted; charging remaining " + pending.size()
            + " op(s) as failed.");
        break;
      }
      final List<BulkOperation> batch = pending;
      try {
        BulkResponse response = esClient.bulk(b -> b.operations(batch));
        pending = classifyAndCount(batch, response);
        if (pending.isEmpty()) {
          final int n = batch.size();
          final int a = attempt;
          LOGGER.info(() -> "Bulk request " + executionId + " recovered on retry attempt "
              + a + " (" + n + " op(s) re-applied).");
          return;
        }
      } catch (Exception e) {
        if (!isRetryable(e)) {
          // The whole resubmit hit a non-retryable error — count all pending as failed and stop.
          LOGGER.severe(() -> "Retry of bulk request " + executionId
              + " hit a non-retryable error (" + describe(e) + "); counting "
              + batch.size() + " op(s) as failed.");
          batch.forEach(op -> recordFailure());
          return;
        }
        final int a = attempt;
        LOGGER.warning(() -> "Retry attempt " + a + " for bulk request " + executionId
            + " failed transiently (" + describe(e) + ").");
        // keep `pending` as-is for the next attempt
      }
    }
    // Retries exhausted (or interrupted): count whatever is still pending as failed.
    if (!pending.isEmpty()) {
      final int n = pending.size();
      LOGGER.warning(() -> "Bulk request " + executionId + " exhausted retries; counting "
          + n + " op(s) as failed.");
      pending.forEach(op -> recordFailure());
    }
  }

  /**
   * Classify a bulk response per op: count successes as indexed, charge per-item 4xx as failed, and
   * return the ops to retry (per-item 429/5xx, or no matching item in a short response — kept so an
   * op can never vanish from the emitted/indexed/failed accounting). Walks per op, not per item.
   */
  private List<BulkOperation> classifyAndCount(List<BulkOperation> operations, BulkResponse response) {
    List<BulkOperation> retryable = new ArrayList<>();
    List<BulkResponseItem> items = response.items();
    for (int i = 0; i < operations.size(); i++) {
      BulkResponseItem item = i < items.size() ? items.get(i) : null;
      if (item == null || (item.error() != null && isRetryableStatus(item.status()))) {
        retryable.add(operations.get(i));
      } else if (item.error() == null) {
        indexedCount.increment();
      } else {
        recordFailure();
        LOGGER.warning(() -> "Failed to index id=" + item.id() + " into " + item.index()
            + ": " + item.error().reason());
      }
    }
    return retryable;
  }

  /** Exponential backoff with full jitter, capped. attempt is 1-based. */
  static long backoffMillis(int attempt) {
    long exp = BASE_BACKOFF_MILLIS << (attempt - 1); // 1s,2s,4s,8s,16s,...
    long capped = Math.min(exp, MAX_BACKOFF_MILLIS);
    // Full jitter in [capped/2, capped] de-synchronises retries across the bulk workers.
    long half = capped / 2;
    return half + ThreadLocalRandom.current().nextLong(half + 1);
  }

  /**
   * Retryable = transient transport/availability problems a resubmit can fix: connection/socket
   * timeouts, connection refused, generic IO, and HTTP 429/502/503/504. Non-retryable = 4xx
   * request/data errors (esp. 400 mapping/parse) which would loop forever.
   */
  static boolean isRetryable(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof SocketTimeoutException
          || c instanceof ConnectTimeoutException
          || c instanceof ConnectException) {
        return true;
      }
      if (c instanceof ResponseException re) {
        return isRetryableStatus(re.getResponse().getStatusLine().getStatusCode());
      }
      if (c instanceof ElasticsearchException ese) {
        return isRetryableStatus(ese.status());
      }
      if (c instanceof IOException) {
        // Generic IO (connection reset, broken pipe, premature EOF) — transient.
        return true;
      }
      if (c.getCause() == c) {
        break; // guard against self-referential cause chains
      }
    }
    return false;
  }

  static boolean isRetryableStatus(int status) {
    return status == 429 || status == 502 || status == 503 || status == 504;
  }

  private static String describe(Throwable t) {
    String msg = t.getMessage();
    return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
  }
}
