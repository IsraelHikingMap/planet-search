package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;

import org.apache.http.StatusLine;
import org.apache.http.conn.ConnectTimeoutException;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;

@Tag("unit")
class IndexerResilienceTest {

    private static final String POINTS_INDEX = "points";

    private IndexingStats stats;

    @BeforeEach
    void setUp() {
        stats = new IndexingStats();
    }

    private long indexed() {
        return stats.indexedCount.sum();
    }

    private long failed() {
        return stats.failedCount.sum();
    }

    // Replays scripted responses/throwables in order, repeating the last once exhausted.
    private static ElasticsearchClient mockClient(Object... script) {
        ElasticsearchClient client = mock(ElasticsearchClient.class);
        Deque<Object> queue = new ArrayDeque<>(List.of(script));
        Answer<BulkResponse> answer = invocation -> {
            Object next = queue.size() > 1 ? queue.poll() : queue.peek();
            if (next instanceof Throwable t) {
                if (t instanceof RuntimeException re) {
                    throw re;
                }
                if (t instanceof Exception e) {
                    throw e;
                }
                throw new RuntimeException(t);
            }
            return (BulkResponse) next;
        };
        try {
            when(client.bulk(any(BulkRequest.class))).thenAnswer(answer);
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
        return client;
    }

    private AccountingBulkListener driveRetryViaAfterBulk(ElasticsearchClient client, int opCount, Throwable wholeBatchFailure) {
        AccountingBulkListener listener = new AccountingBulkListener(client, stats, 0L, 0L);
        listener.afterBulk(1L, requestOf(operations(opCount)), List.of(), wholeBatchFailure);
        listener.awaitRetries();
        return listener;
    }

    @Test
    void classification_transientTransportErrorsAreRetryable() {
        assertTrue(AccountingBulkListener.isRetryable(
                new SocketTimeoutException("30,000 milliseconds timeout on connection")));
        assertTrue(AccountingBulkListener.isRetryable(new ConnectException("refused")));
        assertTrue(AccountingBulkListener.isRetryable(new IOException("connection reset")));
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
    void classification_connectTimeoutException_isRetryable() {
        // org.apache.http.conn.ConnectTimeoutException is a distinct cause-chain arm from
        // SocketTimeoutException/ConnectException; cover it directly (nested too).
        assertTrue(AccountingBulkListener.isRetryable(new ConnectTimeoutException("connect timed out")));
        assertTrue(AccountingBulkListener.isRetryable(
                new RuntimeException("wrapped", new ConnectTimeoutException("connect timed out"))));
    }

    @Test
    void classification_responseException_retryableVsNonRetryableStatus() {
        // ResponseException is final and reads its Response in the ctor, so mock the chain
        // it walks: getResponse().getStatusLine().getStatusCode().
        assertTrue(AccountingBulkListener.isRetryable(responseException(503)),
                "a 503 ResponseException is retryable");
        assertFalse(AccountingBulkListener.isRetryable(responseException(400)),
                "a 400 ResponseException is not retryable");
    }

    @Test
    void classification_elasticsearchExceptionStatus_isHonored() {
        assertTrue(AccountingBulkListener.isRetryable(
                new ElasticsearchException("bulk", errorResponse(429))));
        assertFalse(AccountingBulkListener.isRetryable(
                new ElasticsearchException("bulk", errorResponse(404))));
    }

    @Test
    void classification_selfReferentialCause_doesNotLoopAndIsNotRetryable() {
        // A throwable whose cause is itself must terminate the walk via the guard and
        // fall through to non-retryable rather than spin forever. The JDK forbids
        // initCause(this), so override getCause() to return the instance.
        RuntimeException loop = new RuntimeException("self") {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };
        assertFalse(AccountingBulkListener.isRetryable(loop));
    }

    @Test
    void backoff_isExponentialBoundedAndCapped() {
        assertWithinJitter(1, 1_000);
        assertWithinJitter(2, 2_000);
        assertWithinJitter(3, 4_000);
        assertWithinJitter(4, 8_000);
        assertWithinJitter(5, 16_000);
        assertWithinJitter(6, 16_000);
        assertWithinJitter(10, 16_000);
    }

    private static void assertWithinJitter(int attempt, long cap) {
        for (int i = 0; i < 200; i++) {
            long b = AccountingBulkListener.backoffMillis(attempt, 1_000L, 16_000L);
            assertTrue(b >= cap / 2 && b <= cap,
                    "attempt " + attempt + " backoff " + b + " out of [" + (cap / 2) + "," + cap + "]");
        }
    }

    @Test
    void retry_recoversOnSecondAttempt_countsIndexed_noFailures() {
        ElasticsearchClient client = mockClient(new SocketTimeoutException("timeout"), successResponse(3));

        driveRetryViaAfterBulk(client, 3, new SocketTimeoutException("timeout"));

        assertEquals(3, indexed(), "recovered ops are counted as indexed");
        assertEquals(0, failed());
    }

    @Test
    void retry_perItem4xxIsCountedFailed_andNotRetried() {
        BulkResponse mixed = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item("op-0", 200, false),
                item("op-1", 400, true),
                item("op-2", 200, false))));
        ElasticsearchClient client = mockClient(mixed);

        driveRetryViaAfterBulk(client, 3, new SocketTimeoutException("timeout"));

        assertEquals(2, indexed());
        assertEquals(1, failed(), "a 400 per-item error is counted as failed");
    }

    @Test
    void retry_exhausted_countsFailed() {
        ElasticsearchClient client = mockClient(new SocketTimeoutException("timeout"));

        driveRetryViaAfterBulk(client, 4, new SocketTimeoutException("timeout"));

        assertEquals(0, indexed());
        assertEquals(4, failed(), "exhausted ops are counted as failed");
    }

    @Test
    void retry_partialRetryableItems_areRetriedThenCountedFailed() {
        BulkResponse firstAttempt = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item("op-0", 200, false),
                item("op-1", 503, true))));
        BulkResponse stillFailing = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item("op-1", 503, true))));
        ElasticsearchClient client = mockClient(firstAttempt, stillFailing);

        driveRetryViaAfterBulk(client, 2, new SocketTimeoutException("timeout"));

        assertEquals(1, indexed(), "the op that succeeded on attempt 1 is counted once");
        assertEquals(1, failed(), "the persistently-503 op is counted failed after retries");
    }

    @Test
    void retry_shortResponse_doesNotSilentlyDropSurplusOps() {
        BulkResponse shortOk = BulkResponse.of(b -> b.took(1).errors(false).items(List.of(
                item("op-0", 200, false),
                item("op-1", 200, false))));
        ElasticsearchClient client = mockClient(shortOk, successResponse(1));

        driveRetryViaAfterBulk(client, 3, new SocketTimeoutException("timeout"));

        assertEquals(3, indexed(), "the surplus op is retried and indexed, not dropped");
        assertEquals(0, failed());
    }

    @Test
    void retry_nonRetryableExceptionDuringResubmit_countsFailed_andStops() {
        ElasticsearchClient client = mockClient(new RuntimeException("400 illegal_argument_exception"));

        driveRetryViaAfterBulk(client, 3, new SocketTimeoutException("timeout"));

        assertEquals(3, failed(), "a non-retryable resubmit error counts ops as failed");
        assertEquals(0, indexed());
    }

    @Test
    void retryablePerItemStatus_isRetried_thenRecovers_notCountedFailed() {
        BulkResponse first503 = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item("op-0", 503, true))));
        ElasticsearchClient client = mockClient(first503, successResponse(1));

        driveRetryViaAfterBulk(client, 1, new SocketTimeoutException("timeout"));

        assertEquals(1, indexed(), "the 503 op recovers on resubmit");
        assertEquals(0, failed(), "a 503 is retryable, never charged failed when it recovers");
    }

    @Test
    void wholeBatchNonRetryableThrowable_isNotRetried_countsFailed() {
        AccountingBulkListener listener = new AccountingBulkListener(mock(ElasticsearchClient.class), stats);

        listener.afterBulk(6L, requestOf(operations(2)), List.of(),
                new ElasticsearchException("bulk", errorResponse(400)));
        listener.awaitRetries();

        assertEquals(2, failed());
        assertEquals(0, indexed());
    }

    @Test
    void initialResponse_perItem4xxIsCountedFailed() {
        AccountingBulkListener listener = new AccountingBulkListener(null, stats);
        BulkResponse mixed = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item("op-0", 200, false),
                item("op-1", 400, true),
                item("op-2", 200, false))));

        listener.afterBulk(1L, requestOf(operations(3)), List.of(), mixed);

        assertEquals(2, indexed());
        assertEquals(1, failed(), "a 400 per-item error is counted as failed");
    }

    @Test
    void initialResponse_shortButErrorFree_doesNotTakeFastPath_andRetriesSurplus() {
        // !errors() but items.size() < operations.size(): the fast path must be skipped so
        // the missing op is classified (retried), not silently counted.
        BulkResponse shortOk = BulkResponse.of(b -> b.took(1).errors(false).items(List.of(
                item("op-0", 200, false),
                item("op-1", 200, false))));
        ElasticsearchClient client = mockClient(shortOk, successResponse(1));
        AccountingBulkListener listener = new AccountingBulkListener(client, stats, 0L, 0L);

        listener.afterBulk(1L, requestOf(operations(3)), List.of(), shortOk);
        listener.awaitRetries();

        assertEquals(3, indexed(), "the op missing from the short response is retried and indexed");
        assertEquals(0, failed());
    }

    @Test
    void initialResponse_retryablePerItem_offloadsRetry_thenRecovers() {
        // A retryable (503) per-item on the FIRST response must drive offloadRetry from the
        // success-overload afterBulk, not just from the throwable overload.
        BulkResponse first = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item("op-0", 200, false),
                item("op-1", 503, true))));
        ElasticsearchClient client = mockClient(successResponse(1));
        AccountingBulkListener listener = new AccountingBulkListener(client, stats, 0L, 0L);

        listener.afterBulk(1L, requestOf(operations(2)), List.of(), first);
        listener.awaitRetries();

        assertEquals(2, indexed(), "op-0 indexed immediately, op-1 recovers on retry");
        assertEquals(0, failed());
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

    // ResponseException is final and its real ctor reads the Response body; mock only the
    // accessor chain isRetryable walks.
    private static ResponseException responseException(int status) {
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(status);
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        ResponseException re = mock(ResponseException.class);
        when(re.getResponse()).thenReturn(response);
        return re;
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

    @Test
    void drainTimeout_forcedShutdown_chargesEveryRetryOpAsFailed() throws InterruptedException {
        CountDownLatch release = new CountDownLatch(1);
        ElasticsearchClient blocking = mock(ElasticsearchClient.class);
        try {
            when(blocking.bulk(any(BulkRequest.class))).thenAnswer(inv -> {
                release.await();
                return successResponse(1);
            });
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }

        AccountingBulkListener listener = new AccountingBulkListener(blocking, stats, 0L, 0L, 0L);
        int queuedBatches = 2;
        int batches = AccountingBulkListener.RETRY_POOL_SIZE + queuedBatches;
        for (int i = 0; i < batches; i++) {
            listener.afterBulk(i, requestOf(operations(2)), List.of(), new SocketTimeoutException("timeout"));
        }

        listener.awaitRetries();
        assertTrue(failed() >= 2L * queuedBatches,
                "chargeDropped must charge the queued-never-started batches synchronously on forced shutdown");
        release.countDown();

        long total = 2L * batches;
        for (int i = 0; i < 500 && failed() < total; i++) {
            Thread.sleep(5);
        }
        assertEquals(total, failed(),
                "every retry op — dropped-queued and interrupted-running — is charged failed; none vanish");
        assertEquals(0, indexed());
    }
}
