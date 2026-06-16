package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
 * Step D — indexer resilience. Exercises the three new behaviours without a live
 * Elasticsearch:
 * <ol>
 *   <li>retry classification (which Throwables/HTTP statuses are retryable);</li>
 *   <li>the bounded retry/backoff in the whole-batch (Throwable) path — recovery,
 *       per-item 4xx NOT retried, and transient-bucket charging once exhausted;</li>
 *   <li>the two-bucket guard thresholds and the post-build reconcile gate.</li>
 * </ol>
 */
@Tag("unit")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IndexerResilienceTest {

    private static final String POINTS_INDEX = "points";
    private static final String BBOX_INDEX = "bbox";

    @Mock
    private ElasticsearchClient esClient;

    private LongAdder indexedCount;
    private LongAdder failedPointsCount;
    private LongAdder failedBboxCount;
    private LongAdder transientPointsCharges;
    private LongAdder transientBboxCharges;
    private AccountingBulkListener listener;

    @BeforeEach
    void setUp() {
        indexedCount = new LongAdder();
        failedPointsCount = new LongAdder();
        failedBboxCount = new LongAdder();
        transientPointsCharges = new LongAdder();
        transientBboxCharges = new LongAdder();
        listener = new AccountingBulkListener(
                esClient, indexedCount, failedPointsCount, failedBboxCount,
                transientPointsCharges, transientBboxCharges, POINTS_INDEX, millis -> { /* no sleep */ });
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
    void retry_recoversOnSecondAttempt_countsIndexed_noCharges() throws Exception {
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);

        // First resubmit throws a transient timeout; second succeeds for all 3 ops.
        when(esClient.bulk(this.<BulkRequest>anyBulkFn()))
                .thenThrow(new SocketTimeoutException("timeout"))
                .thenReturn(successResponse(3));

        listener.afterBulk(1L, request, List.of(), new SocketTimeoutException("30s timeout"));

        assertEquals(3, indexedCount.sum(), "recovered ops are counted as indexed");
        assertEquals(0, totalFailed());
        assertEquals(0, transientCharges());
        verify(esClient, times(2)).bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void retry_perItem4xxIsChargedGenuine_andNotRetried() throws Exception {
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);

        // Resubmit returns: op0 ok, op1 a 400 mapping error, op2 ok.
        BulkResponse mixed = BulkResponse.of(b -> b.took(1).errors(true).items(List.of(
                item(POINTS_INDEX, "op-0", 200, false),
                item(POINTS_INDEX, "op-1", 400, true),
                item(POINTS_INDEX, "op-2", 200, false))));
        when(esClient.bulk(this.<BulkRequest>anyBulkFn())).thenReturn(mixed);

        listener.afterBulk(2L, request, List.of(), new SocketTimeoutException("timeout"));

        assertEquals(2, indexedCount.sum());
        assertEquals(1, failedPointsCount.sum(), "a 400 per-item error is a genuine data failure");
        assertEquals(0, transientCharges(), "a 4xx item must NOT end up in the transient bucket");
        // Exactly ONE resubmit — the 400 item must not be retried in a loop.
        verify(esClient, times(1)).bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void retry_exhausted_chargesTransientBucket_pointsClassified() throws Exception {
        BulkRequest request = requestWithOperations(4, POINTS_INDEX);

        // Every resubmit times out — after MAX_RETRY_ATTEMPTS the ops go transient.
        when(esClient.bulk(this.<BulkRequest>anyBulkFn()))
                .thenThrow(new SocketTimeoutException("timeout"));

        listener.afterBulk(3L, request, List.of(), new SocketTimeoutException("timeout"));

        assertEquals(0, indexedCount.sum());
        assertEquals(0, failedPointsCount.sum(), "transient timeouts are NOT genuine data failures");
        assertEquals(4, transientPointsCharges.sum(), "exhausted points ops land in the transient bucket");
        verify(esClient, times(AccountingBulkListener.MAX_RETRY_ATTEMPTS))
                .bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void retry_partialRetryableItems_areRetriedThenCharged() throws Exception {
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
        assertEquals(0, failedPointsCount.sum(), "a 503 is retryable, never a genuine failure");
        assertEquals(1, transientPointsCharges.sum(), "the persistently-503 op goes transient after retries");
    }

    @Test
    void retry_shortResponse_doesNotSilentlyDropSurplusOps() throws Exception {
        // A resubmit of 3 ops returns a response with only 2 items and errors()==false. The surplus
        // op (op-2) has no matching item: it must be RETRIED, never silently dropped (that would lose
        // a doc from the indexed/failed/transient accounting). The second resubmit indexes all 3.
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);
        BulkResponse shortOk = BulkResponse.of(b -> b.took(1).errors(false).items(List.of(
                item(POINTS_INDEX, "op-0", 200, false),
                item(POINTS_INDEX, "op-1", 200, false))));
        when(esClient.bulk(this.<BulkRequest>anyBulkFn()))
                .thenReturn(shortOk)
                .thenReturn(successResponse(1));

        listener.afterBulk(8L, request, List.of(), new SocketTimeoutException("timeout"));

        // 2 indexed on the short attempt + 1 on the retry = 3; nothing lost, nothing charged.
        assertEquals(3, indexedCount.sum(), "the surplus op is retried and indexed, not dropped");
        assertEquals(0, totalFailed());
        assertEquals(0, transientCharges());
    }

    @Test
    void retry_nonRetryableExceptionDuringResubmit_chargesGenuine_andStops() throws Exception {
        BulkRequest request = requestWithOperations(3, POINTS_INDEX);

        when(esClient.bulk(this.<BulkRequest>anyBulkFn()))
                .thenThrow(new RuntimeException("400 illegal_argument_exception"));

        listener.afterBulk(5L, request, List.of(), new SocketTimeoutException("timeout"));

        assertEquals(3, failedPointsCount.sum(), "a non-retryable resubmit error charges genuine failures");
        assertEquals(0, transientCharges());
        verify(esClient, times(1)).bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void wholeBatchNonRetryableThrowable_isNotRetried_chargesGenuine() throws Exception {
        BulkRequest request = requestWithOperations(2, POINTS_INDEX);

        // A 400 ElasticsearchException is non-retryable: charge genuine, never call bulk().
        listener.afterBulk(6L, request, List.of(),
                new ElasticsearchException("bulk", errorResponse(400)));

        assertEquals(2, failedPointsCount.sum());
        assertEquals(0, transientCharges());
        verify(esClient, times(0)).bulk(this.<BulkRequest>anyBulkFn());
    }

    @Test
    void retry_bboxOps_chargedToTransientBboxBucket() throws Exception {
        BulkRequest request = requestWithOperations(2, BBOX_INDEX);
        when(esClient.bulk(this.<BulkRequest>anyBulkFn()))
                .thenThrow(new SocketTimeoutException("timeout"));

        listener.afterBulk(7L, request, List.of(), new SocketTimeoutException("timeout"));

        assertEquals(2, transientBboxCharges.sum(), "bbox transient charges use the bbox transient bucket");
        assertEquals(0, transientPointsCharges.sum());
    }

    // ---------------------------------------------------------------------------
    // 3) Two-bucket guard thresholds
    // ---------------------------------------------------------------------------

    @Test
    void guardThresholds_genuineIsStrict_transientIsGenerous() {
        // Floors when emitted is tiny.
        assertEquals(50L, IndexingStats.genuineFailureThreshold(0));
        assertEquals(50L, IndexingStats.genuineFailureThreshold(1_000));
        assertEquals(5_000L, IndexingStats.transientChargeThreshold(0));
        assertEquals(5_000L, IndexingStats.transientChargeThreshold(1_000_000));

        // At whole-planet scale (~25M points) the percentages dominate the floor.
        long emitted = 25_000_000L;
        // 0.0001% of 25M = 25 -> still floored to 50 (genuine bar stays strict).
        assertEquals(50L, IndexingStats.genuineFailureThreshold(emitted));
        // 0.05% of 25M = 12,500 -> above the 5,000 floor.
        assertEquals(12_500L, IndexingStats.transientChargeThreshold(emitted));
    }

    @Test
    void guard_161PhantomTransientTimeouts_doNotFailTheBuild_butAMassBreakDoes() {
        // The incident: 161 transient charges on a whole-planet build must be tolerated.
        long emitted = 25_000_000L;
        assertFalse(161 > IndexingStats.transientChargeThreshold(emitted),
                "161 phantom transient charges must be within tolerance");
        // ...but a genuine mass mapping break (say 1% of points) must still fail.
        long massBreak = 250_000L;
        assertTrue(massBreak > IndexingStats.genuineFailureThreshold(emitted),
                "a real mass data break must trip the strict genuine-failure guard");
    }

    // ---------------------------------------------------------------------------
    // 4) Reconcile gate
    // ---------------------------------------------------------------------------

    @Test
    void reconcile_passesWhenLandedMatchesExpected_accountingForDedup() {
        // 1,000,000 emitted, 0 genuine failures; 990,000 live + 10,000 deleted (dedup
        // overwrites) => landed 1,000,000 == expected. No shortfall.
        var stats = new ElasticsearchHelper.IndexDocsStats(990_000, 10_000);
        assertNull(ElasticsearchHelper.reconcileLivePoints(1_000_000, 0, stats, 0.001));
    }

    @Test
    void reconcile_subtractsGenuineFailuresFromExpected() {
        // 1,000,000 emitted, 5,000 genuine failures => expected 995,000. Live 995,000.
        var stats = new ElasticsearchHelper.IndexDocsStats(995_000, 0);
        assertNull(ElasticsearchHelper.reconcileLivePoints(1_000_000, 5_000, stats, 0.001));
    }

    @Test
    void reconcile_failsOnRealSilentLossBeyondTolerance() {
        // 1,000,000 expected but only 998,000 landed => 2,000 short > 0.1% (1,000).
        var stats = new ElasticsearchHelper.IndexDocsStats(998_000, 0);
        String msg = ElasticsearchHelper.reconcileLivePoints(1_000_000, 0, stats, 0.001);
        assertNotNull(msg);
        assertTrue(msg.contains("RECONCILE GATE FAILED"));
    }

    @Test
    void reconcile_withinToleranceDoesNotFail() {
        // 1,000,000 expected, 999,500 landed => 500 short < 0.1% (1,000). OK.
        var stats = new ElasticsearchHelper.IndexDocsStats(999_500, 0);
        assertNull(ElasticsearchHelper.reconcileLivePoints(1_000_000, 0, stats, 0.001));
    }

    @Test
    void reconcile_failsOpenWhenStatsUnavailable() {
        // A null stats (could not probe) must NOT block the build (fail open).
        assertNull(ElasticsearchHelper.reconcileLivePoints(1_000_000, 0, null, 0.001));
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private long totalFailed() {
        return failedPointsCount.sum() + failedBboxCount.sum();
    }

    private long transientCharges() {
        return transientPointsCharges.sum() + transientBboxCharges.sum();
    }

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
