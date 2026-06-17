package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import co.elastic.clients.util.ObjectBuilder;

/**
 * Indexer resilience. Exercises the retry behaviours without a live Elasticsearch:
 *  - retry classification (which Throwables/HTTP statuses are retryable);
 *  - the bounded retry/backoff in the whole-batch (Throwable) path — recovery, per-item 4xx NOT
 *    retried, and the single failed bucket once retries are exhausted;
 *  - the initial 200-with-errors path: per-item 4xx counted as failed, per-item 429/5xx fed
 *    through the backoff path.
 */
@Tag("unit")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IndexerResilienceTest {

    private static final String POINTS_INDEX = "points";

    @Mock
    private ElasticsearchClient esClient;

    private LongAdder indexedCount;
    private LongAdder failedCount;
    private AccountingBulkListener listener;

    @BeforeEach
    void setUp() {
        indexedCount = new LongAdder();
        failedCount = new LongAdder();
        listener = new AccountingBulkListener(esClient, indexedCount, failedCount, millis -> { /* no sleep */ },
                TestExecutors.directExecutor());
    }

    // ---------------------------------------------------------------------------
    // 1) Retry classification
    // ---------------------------------------------------------------------------

    @Test
    void classification_transientTransportErrorsAreRetryable() {
        assertTrue(AccountingBulkListener.isRetryable(
                new SocketTimeoutException("30,000 milliseconds timeout on connection")));
        assertTrue(AccountingBulkListener.isRetryable(new ConnectException("refused")));
        assertTrue(AccountingBulkListener.isRetryable(new IOException("connection reset")));
        // wrapped cause is unwrapped
        assertTrue(AccountingBulkListener.isRetryable(
                new RuntimeException("wrapped", new SocketTimeoutException("timeout"))));
    }

    @Test
    void classification_4xxAndPlainRuntimeAreNotRetryable() {
        // A plain RuntimeException with no transient cause is non-retryable.
        assertFalse(AccountingBulkListener.isRetryable(new RuntimeException("bad request")));
        // HTTP statuses: 5xx/429 retryable, 4xx not.
        assertTrue(AccountingBulkListener.isRetryableStatus(429));
        assertTrue(AccountingBulkListener.isRetryableStatus(502));
        assertTrue(AccountingBulkListener.isRetryableStatus(503));
        assertTrue(AccountingBulkListener.isRetryableStatus(504));
        assertFalse(AccountingBulkListener.isRetryableStatus(400));
        assertFalse(AccountingBulkListener.isRetryableStatus(404));
    }

    @Test
    void backoff_isExponentialBoundedAndCapped() {
        // Full-jitter backoff lies in [capped/2, capped]; capped = min(2^(n-1)*1s, 16s).
        assertWithinJitter(1, 1_000);
        assertWithinJitter(2, 2_000);
        assertWithinJitter(3, 4_000);
        assertWithinJitter(4, 8_000);
        assertWithinJitter(5, 16_000);
        assertWithinJitter(6, 16_000); // capped
        assertWithinJitter(10, 16_000); // still capped, no overflow
    }

    private static void assertWithinJitter(int attempt, long cap) {
        for (int i = 0; i < 200; i++) {
            long b = AccountingBulkListener.backoffMillis(attempt);
            assertTrue(b >= cap / 2 && b <= cap,
                    "attempt " + attempt + " backoff " + b + " out of [" + (cap / 2) + "," + cap + "]");
        }
    }

    // ---------------------------------------------------------------------------
    // 2) Retry/backoff in the whole-batch (Throwable) path
    // ---------------------------------------------------------------------------

    @Test
    void retry_recoversOnSecondAttempt_countsIndexed_noFailures() throws Exception {
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);

        // First resubmit throws a transient timeout; second succeeds for all 3 ops.
        when(esClient.bulk(this.<BulkRequest>anyBulkFn()))
                .thenThrow(new SocketTimeoutException("timeout"))
                .thenReturn(successResponse(3));

        listener.afterBulk(1L, request, List.of(), new SocketTimeoutException("30s timeout"));

        assertEquals(3, indexedCount.sum(), "recovered ops are counted as indexed");
        assertEquals(0, failedCount.sum());
        verify(esClient, times(2)).bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void retry_perItem4xxIsCountedFailed_andNotRetried() throws Exception {
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);

        // Resubmit returns: op0 ok, op1 a 400 mapping error, op2 ok.
        BulkResponse mixed = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item(POINTS_INDEX, "op-0", 200, false),
                item(POINTS_INDEX, "op-1", 400, true),
                item(POINTS_INDEX, "op-2", 200, false))));
        when(esClient.bulk(this.<BulkRequest>anyBulkFn())).thenReturn(mixed);

        listener.afterBulk(2L, request, List.of(), new SocketTimeoutException("timeout"));

        assertEquals(2, indexedCount.sum());
        assertEquals(1, failedCount.sum(), "a 400 per-item error is counted as failed");
        // Exactly ONE resubmit — the 400 item must not be retried in a loop.
        verify(esClient, times(1)).bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void retry_exhausted_countsFailed() throws Exception {
        BulkRequest request = requestWithOperations(4, POINTS_INDEX);

        // Every resubmit times out — after MAX_RETRY_ATTEMPTS the ops are counted as failed.
        when(esClient.bulk(this.<BulkRequest>anyBulkFn()))
                .thenThrow(new SocketTimeoutException("timeout"));

        listener.afterBulk(3L, request, List.of(), new SocketTimeoutException("timeout"));

        assertEquals(0, indexedCount.sum());
        assertEquals(4, failedCount.sum(), "exhausted ops are counted as failed");
        verify(esClient, times(AccountingBulkListener.MAX_RETRY_ATTEMPTS))
                .bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void retry_partialRetryableItems_areRetriedThenCountedFailed() throws Exception {
        BulkRequest request = requestWithOperations(2, POINTS_INDEX);

        // First attempt: op0 ok, op1 a 503 (retryable). Next attempts: 503 for the lone op.
        BulkResponse firstAttempt = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item(POINTS_INDEX, "op-0", 200, false),
                item(POINTS_INDEX, "op-1", 503, true))));
        BulkResponse stillFailing = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item(POINTS_INDEX, "op-1", 503, true))));
        when(esClient.bulk(this.<BulkRequest>anyBulkFn()))
                .thenReturn(firstAttempt)
                .thenReturn(stillFailing, stillFailing, stillFailing, stillFailing);

        listener.afterBulk(4L, request, List.of(), new SocketTimeoutException("timeout"));

        assertEquals(1, indexedCount.sum(), "the op that succeeded on attempt 1 is counted once");
        assertEquals(1, failedCount.sum(), "the persistently-503 op is counted failed after retries");
    }

    @Test
    void retry_shortResponse_doesNotSilentlyDropSurplusOps() throws Exception {
        // A resubmit of 3 ops returns a response with only 2 items and errors()==false. The surplus
        // op (op-2) has no matching item: it must be RETRIED, never silently dropped (that would lose
        // a doc from the indexed/failed accounting). The second resubmit indexes all 3.
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);
        BulkResponse shortOk = BulkResponse.of(b -> b.took(1).errors(false).items(List.of(
                item(POINTS_INDEX, "op-0", 200, false),
                item(POINTS_INDEX, "op-1", 200, false))));
        when(esClient.bulk(this.<BulkRequest>anyBulkFn()))
                .thenReturn(shortOk)
                .thenReturn(successResponse(1));

        listener.afterBulk(8L, request, List.of(), new SocketTimeoutException("timeout"));

        // 2 indexed on the short attempt + 1 on the retry = 3; nothing lost, nothing failed.
        assertEquals(3, indexedCount.sum(), "the surplus op is retried and indexed, not dropped");
        assertEquals(0, failedCount.sum());
    }

    @Test
    void retry_nonRetryableExceptionDuringResubmit_countsFailed_andStops() throws Exception {
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);

        when(esClient.bulk(this.<BulkRequest>anyBulkFn()))
                .thenThrow(new RuntimeException("400 illegal_argument_exception"));

        listener.afterBulk(5L, request, List.of(), new SocketTimeoutException("timeout"));

        assertEquals(3, failedCount.sum(), "a non-retryable resubmit error counts ops as failed");
        verify(esClient, times(1)).bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void wholeBatchNonRetryableThrowable_isNotRetried_countsFailed() throws Exception {
        BulkRequest request = requestWithOperations(2, POINTS_INDEX);

        // A 400 ElasticsearchException is non-retryable: count as failed, never call bulk().
        listener.afterBulk(6L, request, List.of(),
                new ElasticsearchException("bulk", errorResponse(400)));

        assertEquals(2, failedCount.sum());
        verify(esClient, times(0)).bulk(this.<BulkRequest>anyBulkFn());
    }

    // ---------------------------------------------------------------------------
    // 2b) Initial bulk-response path (200-with-errors, no Throwable)
    // ---------------------------------------------------------------------------

    @Test
    void initialResponse_perItem4xxIsCountedFailed_notRetried() throws Exception {
        // A 200-with-errors response: op0 ok, op1 a 400 mapping error, op2 ok. The 400 is a
        // failure and must not be resubmitted.
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);
        BulkResponse mixed = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item(POINTS_INDEX, "op-0", 200, false),
                item(POINTS_INDEX, "op-1", 400, true),
                item(POINTS_INDEX, "op-2", 200, false))));

        listener.afterBulk(1L, request, List.of(), mixed);

        assertEquals(2, indexedCount.sum());
        assertEquals(1, failedCount.sum(), "a 400 per-item error is counted as failed");
        verify(esClient, times(0)).bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void initialResponse_perItemRetryableStatusIsResubmitted_notCountedFailed() throws Exception {
        // The bug_002 regression: a per-item 503 inside a 200-with-errors response is retryable —
        // it must go through the backoff path, NOT straight into the failed bucket.
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);
        BulkResponse mixed = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item(POINTS_INDEX, "op-0", 200, false),
                item(POINTS_INDEX, "op-1", 503, true),
                item(POINTS_INDEX, "op-2", 200, false))));
        // The lone retried op succeeds on resubmit.
        when(esClient.bulk(this.<BulkRequest>anyBulkFn())).thenReturn(successResponse(1));

        listener.afterBulk(2L, request, List.of(), mixed);

        assertEquals(3, indexedCount.sum(), "2 indexed on the initial pass + 1 recovered on resubmit");
        assertEquals(0, failedCount.sum(), "a 503 is retryable, never charged failed when it recovers");
        verify(esClient, times(1)).bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void initialResponse_retryablePersists_countsFailedAfterRetries() throws Exception {
        // A per-item 503 that never recovers exhausts retries and is counted as failed.
        BulkRequest request = requestWithOperations(2, POINTS_INDEX);
        BulkResponse mixed = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item(POINTS_INDEX, "op-0", 200, false),
                item(POINTS_INDEX, "op-1", 503, true))));
        BulkResponse stillFailing = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item(POINTS_INDEX, "op-1", 503, true))));
        when(esClient.bulk(this.<BulkRequest>anyBulkFn())).thenReturn(stillFailing);

        listener.afterBulk(3L, request, List.of(), mixed);

        assertEquals(1, indexedCount.sum());
        assertEquals(1, failedCount.sum(), "it is counted failed after retries are exhausted");
    }

    @Test
    void initialResponse_shortResponse_doesNotSilentlyDropSurplusOps() throws Exception {
        // 3 ops, 2-item response: op-0 ok, op-1 400, op-2 has no item -> must be retried, not dropped.
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);
        BulkResponse shortErrors = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item(POINTS_INDEX, "op-0", 200, false),
                item(POINTS_INDEX, "op-1", 400, true))));
        when(esClient.bulk(this.<BulkRequest>anyBulkFn())).thenReturn(successResponse(1));

        listener.afterBulk(9L, request, List.of(), shortErrors);

        assertEquals(2, indexedCount.sum(), "op-0 indexed initially + op-2 recovered on resubmit");
        assertEquals(1, failedCount.sum(), "op-1's 400 is the only genuine failure");
        assertEquals(3, indexedCount.sum() + failedCount.sum(), "no surplus op was dropped");
        verify(esClient, times(1)).bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void initialResponse_shortSuccessResponse_retriesSurplusOps_notJustItemCount() throws Exception {
        // errors()==false but only 2 items for 3 ops: fast path must retry the surplus op, not drop it.
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);
        BulkResponse shortOk = BulkResponse.of(b -> b.took(1).errors(false).items(List.of(
                item(POINTS_INDEX, "op-0", 200, false),
                item(POINTS_INDEX, "op-1", 200, false))));
        when(esClient.bulk(this.<BulkRequest>anyBulkFn())).thenReturn(successResponse(1));

        listener.afterBulk(11L, request, List.of(), shortOk);

        assertEquals(3, indexedCount.sum(), "2 indexed on the short success + 1 surplus op recovered on retry");
        assertEquals(0, failedCount.sum());
        assertEquals(3, indexedCount.sum() + failedCount.sum(), "no surplus op dropped on the no-error path");
    }

    @Test
    void awaitRetries_blocksUntilOffloadedRetryCompletes() throws Exception {
        // Real background pool: afterBulk returns before the retry lands; awaitRetries() must block until it does.
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        var started = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        // Signal entry into backoff, then park — keeps the retry in-flight while we assert nothing counted yet.
        AccountingBulkListener.Sleeper gatedSleep = millis -> {
            started.countDown();
            release.await();
        };
        var bg = new AccountingBulkListener(esClient, indexedCount, failedCount, gatedSleep, pool);
        when(esClient.bulk(this.<BulkRequest>anyBulkFn())).thenReturn(successResponse(2));
        BulkRequest request = requestWithOperations(2, POINTS_INDEX);

        try {
            bg.afterBulk(10L, request, List.of(), new SocketTimeoutException("timeout"));
            // Bounded wait so a never-starting retry fails the test instead of hanging the suite.
            assertTrue(started.await(10, java.util.concurrent.TimeUnit.SECONDS),
                    "the offloaded retry should reach the backoff sleep off the calling thread");
            assertEquals(0, indexedCount.sum(), "retry has not run yet — afterBulk returned without blocking");

            release.countDown();
            bg.awaitRetries();   // must block until the retry completes

            assertEquals(2, indexedCount.sum(), "awaitRetries drained the in-flight retry before returning");
            assertEquals(0, failedCount.sum());
        } finally {
            release.countDown();      // unblock any parked task on an assertion-failure path
            pool.shutdownNow();       // never leak the pool even if awaitRetries() wasn't reached
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** Typed any() for the Function form of esClient.bulk(...). */
    private <T> Function<BulkRequest.Builder, ObjectBuilder<BulkRequest>> anyBulkFn() {
        return any();
    }

    private static BulkResponse successResponse(int n) {
        List<BulkResponseItem> items = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            items.add(item(POINTS_INDEX, "op-" + i, 200, false));
        }
        return BulkResponse.of(b -> b.took(1).errors(false).items(items));
    }

    private static BulkResponseItem item(String index, String id, int status, boolean withError) {
        return BulkResponseItem.of(b -> {
            b.operationType(OperationType.Index).index(index).id(id).status(status);
            if (withError) {
                b.error(e -> e.type("mapper_parsing_exception").reason("err " + id));
            }
            return b;
        });
    }

    private static co.elastic.clients.elasticsearch._types.ErrorResponse errorResponse(int status) {
        return co.elastic.clients.elasticsearch._types.ErrorResponse.of(b -> b
                .status(status)
                .error(e -> e.type("illegal_argument_exception").reason("bad request")));
    }

    private static BulkRequest requestWithOperations(int n, String index) {
        List<BulkOperation> ops = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            ops.add(BulkOperation.of(op -> op.index(ix -> ix.index(index).id("op-" + idx).document(Map.of("k", idx)))));
        }
        return BulkRequest.of(b -> b.operations(ops));
    }
}
