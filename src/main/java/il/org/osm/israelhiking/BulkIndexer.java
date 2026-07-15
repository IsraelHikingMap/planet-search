package il.org.osm.israelhiking;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.http.conn.ConnectTimeoutException;
import org.elasticsearch.client.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.transport.BackoffPolicy;

/**
 * Owns a bulk ingester and drives it losslessly: documents are added through
 * {@link #add}, per-index counters record what was emitted, indexed and
 * dropped, transient failures are retried with backoff, and {@link #close}
 * drains everything before the caller decides what to do with the result. It is
 * index-agnostic — it counts whatever indices the documents name, and leaves
 * the meaning of those indices to the caller.
 */
final class BulkIndexer implements BulkListener<Void>, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BulkIndexer.class);

  static final int MAX_RETRY_ATTEMPTS = 5;
  static final long BASE_BACKOFF_MILLIS = 1_000L;
  static final long MAX_BACKOFF_MILLIS = 16_000L;

  static final int RETRY_POOL_SIZE = 4;
  static final long DRAIN_TIMEOUT_MILLIS = 30 * 60 * 1_000L;

  private final ElasticsearchClient esClient;
  private final Map<String, IndexingStats> statsByIndex = new ConcurrentHashMap<>();
  private final ExecutorService retryExecutor;
  private final AtomicBoolean drained = new AtomicBoolean(false);
  private final AtomicBoolean ingesterClosed = new AtomicBoolean(false);
  private final BulkIngester<Void> bulkIngester;

  BulkIndexer(ElasticsearchClient esClient) {
    this.esClient = esClient;
    this.retryExecutor = Executors.newFixedThreadPool(RETRY_POOL_SIZE, r -> {
      Thread t = new Thread(r, "es-bulk-retry");
      t.setDaemon(true);
      return t;
    });
    this.bulkIngester = BulkIngester.of(b -> b
        .client(esClient)
        .maxOperations(5_000)
        .maxSize(5 * 1024 * 1024)
        .maxConcurrentRequests(4)
        .backoffPolicy(BackoffPolicy.noBackoff())
        .listener(this));
  }

  /**
   * Serialize-and-enqueue a document, counting it as emitted against its index.
   */
  void add(BulkOperation operation) {
    statsFor(bulkOperationIndex(operation)).emitted.increment();
    bulkIngester.add(operation);
  }

  /** Charge one dropped document to its index. */
  void recordFailure(String index) {
    statsFor(index).failed.increment();
  }

  private IndexingStats statsFor(String index) {
    return statsByIndex.computeIfAbsent(index, k -> new IndexingStats());
  }

  @Override
  public void beforeBulk(long executionId, BulkRequest request, List<Void> contexts) {
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, BulkResponse response) {
    List<BulkResponseItem> items = response.items() == null ? List.of() : response.items();
    if (!response.errors() && items.size() == request.operations().size()) {
      for (BulkResponseItem item : items) {
        statsFor(item.index()).indexed.increment();
      }
      return;
    }
    List<BulkOperation> retryable = classifyAndCount(request.operations(), items);
    if (!retryable.isEmpty()) {
      LOGGER.warn("Bulk request {} had {} retryable per-item failure(s); resubmitting with backoff.",
          executionId, retryable.size());
      offloadRetry(executionId, retryable);
    }
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, Throwable failure) {
    if (isRetryable(failure)) {
      LOGGER.warn("Bulk request {} failed transiently ({}); retrying with backoff.",
          executionId, describe(failure));
      offloadRetry(executionId, request.operations());
    } else {
      LOGGER.error("Bulk request {} failed non-retryably ({}); counting {} op(s) as failed.",
          executionId, describe(failure), request.operations().size());
      request.operations().forEach(op -> recordFailure(bulkOperationIndex(op)));
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

  private void offloadRetry(long executionId, List<BulkOperation> operations) {
    List<BulkOperation> batch = new ArrayList<>(operations);
    try {
      retryExecutor.execute(new RetryTask(executionId, batch));
    } catch (RejectedExecutionException ree) {
      LOGGER.error("Retry pool already shut down for bulk request {}; counting {} op(s) as failed.",
          executionId, batch.size());
      batch.forEach(op -> recordFailure(bulkOperationIndex(op)));
    }
  }

  /**
   * Drains all in-flight work and retries, then logs what was indexed per index.
   */
  @Override
  public void close() {
    boolean firstClose = ingesterClosed.compareAndSet(false, true);
    if (firstClose) {
      try {
        bulkIngester.close();
      } catch (Exception e) {
        LOGGER.warn("Bulk ingester close failed: {}", e.getMessage());
      }
    }
    awaitRetries();
    if (firstClose) {
      logSummary();
    }
  }

  private void logSummary() {
    for (var entry : statsByIndex.entrySet()) {
      IndexingStats s = entry.getValue();
      String line = "Indexing finished for " + entry.getKey() + ": emitted=" + s.getEmitted()
          + " indexed=" + s.getIndexed() + " failed=" + s.getFailed() + ".";
      if (s.getFailed() > 0) {
        LOGGER.warn(line);
      } else {
        LOGGER.info(line);
      }
    }
  }


  void awaitRetries() {
    if (!drained.compareAndSet(false, true)) {
      return;
    }
    retryExecutor.shutdown();
    try {
      if (!retryExecutor.awaitTermination(DRAIN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
        LOGGER.error("Bulk retries did not drain within {} ms; forcing shutdown — some re-indexed "
            + "docs may not have landed before the alias swap.", DRAIN_TIMEOUT_MILLIS);
        chargeDropped(retryExecutor.shutdownNow());
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      chargeDropped(retryExecutor.shutdownNow());
    }
  }

  private void chargeDropped(List<Runnable> dropped) {
    dropped.stream()
        .filter(RetryTask.class::isInstance)
        .forEach(r -> ((RetryTask) r).batch.forEach(op -> recordFailure(bulkOperationIndex(op))));
  }

  private void resubmitWithBackoff(long executionId, List<BulkOperation> operations) {
    List<BulkOperation> pending = new ArrayList<>(operations);
    for (int attemptNo = 1; attemptNo <= MAX_RETRY_ATTEMPTS && !pending.isEmpty(); attemptNo++) {
      try {
        Thread.sleep(backoffMillis(attemptNo));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Retry interrupted; charging remaining {} op(s) as failed.", pending.size());
        break;
      }
      final List<BulkOperation> batch = pending;
      try {
        BulkResponse response = esClient.bulk(BulkRequest.of(b -> b.operations(batch)));
        pending = classifyAndCount(batch, response.items() == null ? List.of() : response.items());
        if (pending.isEmpty()) {
          LOGGER.info("Bulk request {} recovered on retry attempt {} ({} op(s) re-applied).",
              executionId, attemptNo, batch.size());
          return;
        }
      } catch (Exception e) {
        if (!isRetryable(e)) {
          LOGGER.error("Retry of bulk request {} hit a non-retryable error ({}); counting {} op(s) as failed.",
              executionId, describe(e), batch.size());
          batch.forEach(op -> recordFailure(bulkOperationIndex(op)));
          return;
        }
        LOGGER.warn("Retry attempt {} for bulk request {} failed transiently ({}).",
            attemptNo, executionId, describe(e));
      }
    }
    if (!pending.isEmpty()) {
      final List<BulkOperation> remaining = pending;
      LOGGER.warn("Bulk request {} exhausted retries; counting {} op(s) as failed.",
          executionId, remaining.size());
      remaining.forEach(op -> recordFailure(bulkOperationIndex(op)));
    }
  }

  private List<BulkOperation> classifyAndCount(List<BulkOperation> operations, List<BulkResponseItem> items) {
    List<BulkOperation> retryable = new ArrayList<>();
    for (int i = 0; i < operations.size(); i++) {
      BulkResponseItem item = i < items.size() ? items.get(i) : null;
      if (item == null || (item.error() != null && isRetryableStatus(item.status()))) {
        retryable.add(operations.get(i));
      } else if (item.error() == null) {
        statsFor(item.index()).indexed.increment();
      } else {
        recordFailure(item.index());
        LOGGER.warn("Failed to index id={} into {}: {}", item.id(), item.index(), item.error().reason());
      }
    }
    return retryable;
  }

  static String bulkOperationIndex(BulkOperation op) {
    if (op.isIndex()) {
      return op.index().index();
    } else if (op.isCreate()) {
      return op.create().index();
    } else if (op.isUpdate()) {
      return op.update().index();
    } else if (op.isDelete()) {
      return op.delete().index();
    }
    return null;
  }

  static long backoffMillis(int attempt) {
    long exp = BASE_BACKOFF_MILLIS << (attempt - 1);
    long capped = Math.min(exp, MAX_BACKOFF_MILLIS);
    long half = capped / 2;
    return half + ThreadLocalRandom.current().nextLong(half + 1);
  }

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
