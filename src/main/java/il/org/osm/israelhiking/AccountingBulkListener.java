package il.org.osm.israelhiking;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

final class AccountingBulkListener implements BulkListener<Void> {
  private static final Logger LOGGER = Logger.getLogger(AccountingBulkListener.class.getName());

  static final int MAX_RETRY_ATTEMPTS = 5;
  static final long DEFAULT_BASE_BACKOFF_MILLIS = 1_000L;
  static final long DEFAULT_MAX_BACKOFF_MILLIS = 16_000L;

  static final int RETRY_POOL_SIZE = 4;
  static final long DEFAULT_DRAIN_TIMEOUT_MILLIS = 30 * 60 * 1_000L;

  private final ElasticsearchClient esClient;
  private final LongAdder indexedCount;
  private final LongAdder failedCount;
  private final ExecutorService retryExecutor;
  private final long baseBackoffMillis;
  private final long maxBackoffMillis;
  private final long drainTimeoutMillis;
  private final AtomicBoolean drained = new AtomicBoolean(false);
  private final AtomicBoolean ingesterClosed = new AtomicBoolean(false);

  AccountingBulkListener(ElasticsearchClient esClient, IndexingStats stats) {
    this(esClient, stats, DEFAULT_BASE_BACKOFF_MILLIS, DEFAULT_MAX_BACKOFF_MILLIS,
        DEFAULT_DRAIN_TIMEOUT_MILLIS);
  }

  AccountingBulkListener(ElasticsearchClient esClient, IndexingStats stats,
      long baseBackoffMillis, long maxBackoffMillis) {
    this(esClient, stats, baseBackoffMillis, maxBackoffMillis, DEFAULT_DRAIN_TIMEOUT_MILLIS);
  }

  AccountingBulkListener(ElasticsearchClient esClient, IndexingStats stats,
      long baseBackoffMillis, long maxBackoffMillis, long drainTimeoutMillis) {
    this.esClient = esClient;
    this.indexedCount = stats.indexedCount;
    this.failedCount = stats.failedCount;
    this.baseBackoffMillis = baseBackoffMillis;
    this.maxBackoffMillis = maxBackoffMillis;
    this.drainTimeoutMillis = drainTimeoutMillis;
    this.retryExecutor = Executors.newFixedThreadPool(RETRY_POOL_SIZE, r -> {
      Thread t = new Thread(r, "es-bulk-retry");
      t.setDaemon(true);
      return t;
    });
  }

  @Override
  public void beforeBulk(long executionId, BulkRequest request, List<Void> contexts) {
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, BulkResponse response) {
    List<BulkResponseItem> items = response.items() == null ? List.of() : response.items();
    if (!response.errors() && items.size() == request.operations().size()) {
      indexedCount.add(items.size());
      return;
    }
    List<BulkOperation> retryable = classifyAndCount(request.operations(), items);
    if (!retryable.isEmpty()) {
      LOGGER.warning(() -> "Bulk request " + executionId + " had " + retryable.size()
          + " retryable per-item failure(s); resubmitting with backoff.");
      offloadRetry(executionId, retryable);
    }
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, Throwable failure) {
    if (isRetryable(failure)) {
      LOGGER.warning(() -> "Bulk request " + executionId + " failed transiently ("
          + describe(failure) + "); retrying with backoff.");
      offloadRetry(executionId, request.operations());
    } else {
      LOGGER.severe(() -> "Bulk request " + executionId + " failed non-retryably ("
          + describe(failure) + "); counting " + request.operations().size() + " op(s) as failed.");
      request.operations().forEach(op -> failedCount.increment());
    }
  }

  private final class RetryTask implements Runnable {
    private final long executionId;
    private final List<BulkOperation> batch;

    RetryTask(long executionId, List<BulkOperation> batch) {
      this.executionId = executionId;
      this.batch = batch;
    }

    @Override
    public void run() {
      resubmitWithBackoff(executionId, batch);
    }
  }

  // Retries run off the ingester scheduler thread so backoff can't head-of-line-block ingest.
  private void offloadRetry(long executionId, List<BulkOperation> operations) {
    List<BulkOperation> batch = new ArrayList<>(operations);
    try {
      retryExecutor.execute(new RetryTask(executionId, batch));
    } catch (RejectedExecutionException ree) {
      LOGGER.severe(() -> "Retry pool already shut down for bulk request " + executionId
          + "; counting " + batch.size() + " op(s) as failed.");
      batch.forEach(op -> failedCount.increment());
    }
  }

  // Must drain before the alias swap, else in-flight retry docs race the swap.
  void awaitRetries() {
    if (!drained.compareAndSet(false, true)) {
      return;
    }
    retryExecutor.shutdown();
    try {
      if (!retryExecutor.awaitTermination(drainTimeoutMillis, TimeUnit.MILLISECONDS)) {
        LOGGER.severe("Bulk retries did not drain within " + drainTimeoutMillis
            + " ms; forcing shutdown — some re-indexed docs may not have landed before the alias swap.");
        chargeDropped(retryExecutor.shutdownNow());
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      chargeDropped(retryExecutor.shutdownNow());
    }
  }

  boolean tryClaimIngesterClose() {
    return ingesterClosed.compareAndSet(false, true);
  }

  private void chargeDropped(List<Runnable> dropped) {
    dropped.stream()
        .filter(RetryTask.class::isInstance)
        .forEach(r -> ((RetryTask) r).batch.forEach(op -> failedCount.increment()));
  }

  private void resubmitWithBackoff(long executionId, List<BulkOperation> operations) {
    List<BulkOperation> pending = new ArrayList<>(operations);
    for (int attemptNo = 1; attemptNo <= MAX_RETRY_ATTEMPTS && !pending.isEmpty(); attemptNo++) {
      try {
        Thread.sleep(backoffMillis(attemptNo, baseBackoffMillis, maxBackoffMillis));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        LOGGER.warning("Retry interrupted; charging remaining " + pending.size()
            + " op(s) as failed.");
        break;
      }
      final List<BulkOperation> batch = pending;
      try {
        BulkResponse response = esClient.bulk(BulkRequest.of(b -> b.operations(batch)));
        pending = classifyAndCount(batch, response.items() == null ? List.of() : response.items());
        if (pending.isEmpty()) {
          final int n = batch.size();
          final int a = attemptNo;
          LOGGER.info(() -> "Bulk request " + executionId + " recovered on retry attempt "
              + a + " (" + n + " op(s) re-applied).");
          return;
        }
      } catch (Exception e) {
        if (!isRetryable(e)) {
          LOGGER.severe(() -> "Retry of bulk request " + executionId
              + " hit a non-retryable error (" + describe(e) + "); counting "
              + batch.size() + " op(s) as failed.");
          batch.forEach(op -> failedCount.increment());
          return;
        }
        final int a = attemptNo;
        LOGGER.warning(() -> "Retry attempt " + a + " for bulk request " + executionId
            + " failed transiently (" + describe(e) + ").");
      }
    }
    if (!pending.isEmpty()) {
      final int n = pending.size();
      LOGGER.warning(() -> "Bulk request " + executionId + " exhausted retries; counting "
          + n + " op(s) as failed.");
      pending.forEach(op -> failedCount.increment());
    }
  }

  private List<BulkOperation> classifyAndCount(List<BulkOperation> operations, List<BulkResponseItem> items) {
    List<BulkOperation> retryable = new ArrayList<>();
    for (int i = 0; i < operations.size(); i++) {
      BulkResponseItem item = i < items.size() ? items.get(i) : null;
      // null: op missing from a short response, retry so it can't vanish from accounting.
      if (item == null || (item.error() != null && isRetryableStatus(item.status()))) {
        retryable.add(operations.get(i));
      } else if (item.error() == null) {
        indexedCount.increment();
      } else {
        failedCount.increment();
        LOGGER.warning(() -> "Failed to index id=" + item.id() + " into " + item.index()
            + ": " + item.error().reason());
      }
    }
    return retryable;
  }

  static long backoffMillis(int attempt, long baseBackoffMillis, long maxBackoffMillis) {
    long exp = baseBackoffMillis << (attempt - 1);
    long capped = Math.min(exp, maxBackoffMillis);
    long half = capped / 2;
    return half + ThreadLocalRandom.current().nextLong(half + 1);
  }

  // Per-item 4xx are non-retryable: resubmitting would loop forever.
  static boolean isRetryable(Throwable t) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable c = t; c != null && seen.add(c); c = c.getCause()) {
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
        return true;
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
