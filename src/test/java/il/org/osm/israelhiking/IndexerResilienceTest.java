package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;

/**
 * Indexer resilience without a live Elasticsearch:
 *  - retry classification (which Throwables/HTTP statuses are retryable);
 *  - the bounded retry/backoff loop — recovery, per-item 4xx NOT retried, the single failed bucket
 *    once retries are exhausted, and that surplus ops in a short response are retried not dropped.
 *
 * The retry loop is the production method resubmitWithBackoff(execId, ops, attempt, sleep); the
 * tests drive it with a stubbed attempt-sequence and a no-op sleep — the same loop production wires
 * to esClient.bulk and Thread::sleep, so no test-only constructor or injected executor is needed.
 */
@Tag("unit")
class IndexerResilienceTest {

    private static final String POINTS_INDEX = "points";

    private IndexingStats stats;
    private AccountingBulkListener listener;

    @BeforeEach
    void setUp() {
        stats = new IndexingStats();
        listener = new AccountingBulkListener(null, stats);
    }

    private long indexed() {
        return stats.indexedCount.sum();
    }

    private long failed() {
        return stats.failedCount.sum();
    }

    /** A no-op backoff so the loop runs instantly. */
    private static final AccountingBulkListener.BackoffSleep NO_SLEEP = millis -> { /* no real delay */ };

    /** An attempt function that returns the queued responses in order, throwing any queued Throwable. */
    private static AccountingBulkListener.BulkAttempt attempts(Object... responsesOrThrowables) {
        Deque<Object> queue = new ArrayDeque<>(List.of(responsesOrThrowables));
        return ops -> {
            Object next = queue.isEmpty() ? responsesOrThrowables[responsesOrThrowables.length - 1] : queue.poll();
            if (next instanceof Throwable t) {
                if (t instanceof Exception e) {
                    throw e;
                }
                throw new RuntimeException(t);
            }
            return (BulkResponse) next;
        };
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
        assertFalse(AccountingBulkListener.isRetryable(new RuntimeException("bad request")));
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
        assertWithinJitter(6, 16_000);
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
    // 2) The retry/backoff loop
    // ---------------------------------------------------------------------------

    @Test
    void retry_recoversOnSecondAttempt_countsIndexed_noFailures() {
        AttemptCounter attempt = new AttemptCounter(
                new SocketTimeoutException("timeout"), successResponse(3));

        listener.resubmitWithBackoff(1L, operations(3), attempt, NO_SLEEP);

        assertEquals(3, indexed(), "recovered ops are counted as indexed");
        assertEquals(0, failed());
        assertEquals(2, attempt.calls);
    }

    @Test
    void retry_perItem4xxIsCountedFailed_andNotRetried() {
        // Resubmit returns: op0 ok, op1 a 400 mapping error, op2 ok.
        BulkResponse mixed = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item("op-0", 200, false),
                item("op-1", 400, true),
                item("op-2", 200, false))));
        AttemptCounter attempt = new AttemptCounter(mixed);

        listener.resubmitWithBackoff(2L, operations(3), attempt, NO_SLEEP);

        assertEquals(2, indexed());
        assertEquals(1, failed(), "a 400 per-item error is counted as failed");
        assertEquals(1, attempt.calls, "the 400 item must not be retried in a loop");
    }

    @Test
    void retry_exhausted_countsFailed() {
        AttemptCounter attempt = new AttemptCounter(new SocketTimeoutException("timeout"));

        listener.resubmitWithBackoff(3L, operations(4), attempt, NO_SLEEP);

        assertEquals(0, indexed());
        assertEquals(4, failed(), "exhausted ops are counted as failed");
        assertEquals(AccountingBulkListener.MAX_RETRY_ATTEMPTS, attempt.calls);
    }

    @Test
    void retry_partialRetryableItems_areRetriedThenCountedFailed() {
        BulkResponse firstAttempt = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item("op-0", 200, false),
                item("op-1", 503, true))));
        BulkResponse stillFailing = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item("op-1", 503, true))));

        listener.resubmitWithBackoff(4L, operations(2), attempts(firstAttempt, stillFailing), NO_SLEEP);

        assertEquals(1, indexed(), "the op that succeeded on attempt 1 is counted once");
        assertEquals(1, failed(), "the persistently-503 op is counted failed after retries");
    }

    @Test
    void retry_shortResponse_doesNotSilentlyDropSurplusOps() {
        // A resubmit of 3 ops returns 2 items, errors()==false. The surplus op (op-2) has no matching
        // item: it must be retried, never silently dropped. The second resubmit indexes it.
        BulkResponse shortOk = BulkResponse.of(b -> b.took(1).errors(false).items(List.of(
                item("op-0", 200, false),
                item("op-1", 200, false))));

        listener.resubmitWithBackoff(8L, operations(3),
                attempts(shortOk, successResponse(1)), NO_SLEEP);

        assertEquals(3, indexed(), "the surplus op is retried and indexed, not dropped");
        assertEquals(0, failed());
    }

    @Test
    void retry_nonRetryableExceptionDuringResubmit_countsFailed_andStops() {
        AttemptCounter attempt = new AttemptCounter(new RuntimeException("400 illegal_argument_exception"));

        listener.resubmitWithBackoff(5L, operations(3), attempt, NO_SLEEP);

        assertEquals(3, failed(), "a non-retryable resubmit error counts ops as failed");
        assertEquals(1, attempt.calls);
    }

    // ---------------------------------------------------------------------------
    // 3) Whole-batch (Throwable) dispatch
    // ---------------------------------------------------------------------------

    @Test
    void wholeBatchNonRetryableThrowable_isNotRetried_countsFailed() {
        // A 400 ElasticsearchException is non-retryable: afterBulk counts it failed without resubmitting.
        listener.afterBulk(6L, requestOf(operations(2)), List.of(),
                new ElasticsearchException("bulk", errorResponse(400)));

        assertEquals(2, failed());
        assertEquals(0, indexed());
    }

    // ---------------------------------------------------------------------------
    // 4) Initial 200-with-errors classification (afterBulk(BulkResponse) -> classifyAndCount)
    // ---------------------------------------------------------------------------

    @Test
    void initialResponse_perItem4xxIsCountedFailed() {
        BulkResponse mixed = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item("op-0", 200, false),
                item("op-1", 400, true),
                item("op-2", 200, false))));

        listener.afterBulk(1L, requestOf(operations(3)), List.of(), mixed);

        assertEquals(2, indexed());
        assertEquals(1, failed(), "a 400 per-item error is counted as failed");
    }

    @Test
    void retryablePerItemStatusIsRetried_notCountedFailed() {
        // The bug_002 regression: a per-item 503 is retryable — it goes through the retry loop, not
        // straight into the failed bucket. Driving the loop directly: the lone 503 op recovers.
        BulkResponse recovered = successResponse(1);

        listener.resubmitWithBackoff(2L,
                List.of(operations(3).get(1)), attempts(recovered), NO_SLEEP);

        assertEquals(1, indexed(), "the 503 op recovers on resubmit");
        assertEquals(0, failed(), "a 503 is retryable, never charged failed when it recovers");
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** An attempt function that returns each queued response once, then repeats the last forever. */
    private static final class AttemptCounter implements AccountingBulkListener.BulkAttempt {
        private final Object[] script;
        int calls = 0;

        AttemptCounter(Object... script) {
            this.script = script;
        }

        @Override
        public BulkResponse submit(List<BulkOperation> operations) throws Exception {
            Object next = script[Math.min(calls, script.length - 1)];
            calls++;
            if (next instanceof Throwable t) {
                if (t instanceof Exception e) {
                    throw e;
                }
                throw new RuntimeException(t);
            }
            return (BulkResponse) next;
        }
    }

    private static BulkResponse successResponse(int n) {
        List<BulkResponseItem> items = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            items.add(item("op-" + i, 200, false));
        }
        return BulkResponse.of(b -> b.took(1).errors(false).items(items));
    }

    private static BulkResponseItem item(String id, int status, boolean withError) {
        return BulkResponseItem.of(b -> {
            b.operationType(OperationType.Index).index(POINTS_INDEX).id(id).status(status);
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

    private static List<BulkOperation> operations(int n) {
        List<BulkOperation> ops = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            ops.add(BulkOperation.of(op -> op.index(ix -> ix.index(POINTS_INDEX).id("op-" + idx).document(Map.of("k", idx)))));
        }
        return ops;
    }

    private static co.elastic.clients.elasticsearch.core.BulkRequest requestOf(List<BulkOperation> ops) {
        return co.elastic.clients.elasticsearch.core.BulkRequest.of(b -> b.operations(ops));
    }
}
