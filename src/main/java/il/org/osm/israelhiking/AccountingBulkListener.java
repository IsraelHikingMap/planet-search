package il.org.osm.israelhiking;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
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

  /** Injectable sleep so the backoff is testable without real delays. */
  @FunctionalInterface
  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final ElasticsearchClient esClient;
  private final LongAdder indexedCount;
  private final LongAdder failedCount;
  private final Sleeper sleeper;

  AccountingBulkListener(ElasticsearchClient esClient, IndexingStats stats) {
    this(esClient, stats.indexedCount, stats.failedCount, Thread::sleep);
  }

  AccountingBulkListener(ElasticsearchClient esClient, LongAdder indexedCount, LongAdder failedCount) {
    this(esClient, indexedCount, failedCount, Thread::sleep);
  }

  // Test seam: lets unit tests inject a no-op/recording Sleeper.
  AccountingBulkListener(ElasticsearchClient esClient, LongAdder indexedCount, LongAdder failedCount,
      Sleeper sleeper) {
    this.esClient = esClient;
    this.indexedCount = indexedCount;
    this.failedCount = failedCount;
    this.sleeper = sleeper;
  }

  private void recordFailure() {
    failedCount.increment();
  }

  @Override
  public void beforeBulk(long executionId, BulkRequest request, List<Void> contexts) {
    // no-op
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, BulkResponse response) {
    if (!response.errors()) {
      indexedCount.add(response.items().size());
      return;
    }
    // A 200-with-errors response can mix indexed items, genuine per-item 4xx (mapping/parse), and
    // retryable per-item statuses (429/5xx from a full bulk queue or a rebalancing shard). The
    // BulkIngester does NOT auto-retry these — only the Throwable overload does — so classify here
    // the same way reconcileRetryResponse does: count successes, charge 4xx as genuine, and feed the
    // retryable subset through the backoff path instead of charging it as a genuine data failure.
    List<BulkOperation> operations = request.operations();
    List<BulkOperation> retryable = new ArrayList<>();
    List<BulkResponseItem> items = response.items();
    for (int i = 0; i < items.size(); i++) {
      BulkResponseItem item = items.get(i);
      if (item.error() == null) {
        indexedCount.increment();
      } else if (isRetryableStatus(item.status()) && i < operations.size()) {
        retryable.add(operations.get(i));
      } else {
        recordFailure();
        LOGGER.warning(() -> "Failed to index id=" + item.id() + " into " + item.index()
            + ": " + item.error().reason());
      }
    }
    if (!retryable.isEmpty()) {
      LOGGER.warning(() -> "Bulk request " + executionId + " had " + retryable.size()
          + " retryable per-item failure(s); resubmitting with backoff.");
      resubmitWithBackoff(executionId, retryable);
    }
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, Throwable failure) {
    // A whole-batch failure has no per-item breakdown, and the 8.x BulkIngester does not
    // auto-resubmit on a Throwable, so classify and (for transient errors) retry the batch with
    // exponential backoff + jitter before charging anything.
    if (isRetryable(failure)) {
      LOGGER.warning(() -> "Bulk request " + executionId + " failed transiently ("
          + describe(failure) + "); retrying with backoff.");
      resubmitWithBackoff(executionId, request.operations());
    } else {
      // Non-retryable whole-batch error (e.g. a 4xx request-level rejection): do NOT retry (would
      // loop forever) — count the ops as failed.
      LOGGER.severe(() -> "Bulk request " + executionId + " failed non-retryably ("
          + describe(failure) + "); counting " + request.operations().size() + " op(s) as failed.");
      request.operations().forEach(op -> recordFailure());
    }
  }

  /**
   * Resubmit a timed-out batch via a fresh synchronous bulk call, with exponential backoff +
   * jitter, up to MAX_RETRY_ATTEMPTS times. Each attempt's response is reconciled per item:
   * successes counted as indexed, per-item 4xx charged as genuine failures (never retried), and
   * only retryable ops carry over. Whatever remains after the final attempt goes to the transient
   * bucket.
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
            + " op(s) as transient.");
        break;
      }
      final List<BulkOperation> batch = pending;
      try {
        BulkResponse response = esClient.bulk(b -> b.operations(batch));
        pending = reconcileRetryResponse(batch, response);
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
   * Classify a retry-attempt's response: count successes, charge per-item 4xx as genuine
   * failures, and return the ops to retry (per-item status 429 or 5xx). An op with no matching
   * response item is conservatively kept for retry.
   */
  private List<BulkOperation> reconcileRetryResponse(List<BulkOperation> batch, BulkResponse response) {
    // Walk per op (not per response item): a response shorter than the batch must not silently
    // drop the surplus. An op with no matching response item is kept for retry so it can never
    // vanish from the emitted/indexed/failed accounting.
    List<BulkOperation> retryAgain = new ArrayList<>();
    List<BulkResponseItem> items = response.items();
    for (int i = 0; i < batch.size(); i++) {
      BulkResponseItem item = i < items.size() ? items.get(i) : null;
      if (item == null) {
        retryAgain.add(batch.get(i)); // no response for this op — retry it, don't drop it
      } else if (item.error() == null) {
        indexedCount.increment();
      } else if (isRetryableStatus(item.status())) {
        retryAgain.add(batch.get(i));
      } else {
        // Genuine per-item failure (e.g. 400 mapping/parse) — must NOT be retried.
        recordFailure();
        LOGGER.warning(() -> "Failed to index id=" + item.id() + " into " + item.index()
            + ": " + item.error().reason());
      }
    }
    return retryAgain;
  }

  // Exponential backoff with full jitter, capped. attempt is 1-based.
  static long backoffMillis(int attempt) {
    long exp = BASE_BACKOFF_MILLIS << (attempt - 1); // 1s,2s,4s,8s,16s,...
    long capped = Math.min(exp, MAX_BACKOFF_MILLIS);
    // Full jitter in [capped/2, capped] keeps it bounded but de-synchronised
    // across the 4 concurrent bulk workers.
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
