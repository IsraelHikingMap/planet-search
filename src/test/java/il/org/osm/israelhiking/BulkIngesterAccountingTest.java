package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;

/**
 * Regression guard for the lossless BulkIngester indexing invariant (Story 0.5).
 *
 * <p>The production listener is the behavior-preserving extract
 * {@link AccountingBulkListener}; the per-item classification
 * logic is identical to the former anonymous inner class. Driving its
 * {@code afterBulk} overloads directly lets us assert, deterministically and
 * without a live Elasticsearch, that:
 * <ol>
 *   <li>AC1 — a bulk item failure is <em>surfaced</em> (counted via failedCount and
 *       logged at WARNING), never swallowed; the success path increments indexedCount.</li>
 *   <li>AC2 — lossless accounting: every item the listener sees is classified as exactly
 *       one of indexed / failed, so indexed + failed == items submitted.</li>
 *   <li>AC3 — a whole-batch failure (the {@code Throwable} overload) adds the entire batch
 *       to failedCount and is logged at SEVERE, and the fail-the-build decision is taken.</li>
 * </ol>
 *
 * <p>Approach: per the story, exercising the real {@code BulkIngester} (approach a) is awkward
 * against the 8.x client because it derives an internal {@code ElasticsearchAsyncClient} from
 * the transport; we use approach (b): a behavior-preserving extract of the listener so it can be
 * unit-constructed and driven directly. The end-to-end reconciliation
 * (emitted == indexed + failed against the live fixture, indexed == ES {@code _count(points)})
 * is deferred to the orchestrator's shared-ES run.
 */
@Tag("unit")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BulkIngesterAccountingTest {

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

    private Logger profileLogger;
    private RecordingHandler handler;
    private Level previousLevel;

    @BeforeEach
    void setUp() {
        indexedCount = new LongAdder();
        failedPointsCount = new LongAdder();
        failedBboxCount = new LongAdder();
        transientPointsCharges = new LongAdder();
        transientBboxCharges = new LongAdder();
        // No-op sleeper so backoff retries run instantly in tests.
        listener = new AccountingBulkListener(
                esClient, indexedCount, failedPointsCount, failedBboxCount,
                transientPointsCharges, transientBboxCharges, POINTS_INDEX, millis -> {});

        // Capture log records so "surfaced" can be asserted (logged AND counted),
        // proving the old catch(Exception){/*swallow*/} pattern is gone.
        profileLogger = Logger.getLogger(PlanetSearchProfile.class.getName());
        previousLevel = profileLogger.getLevel();
        profileLogger.setLevel(Level.ALL);
        handler = new RecordingHandler();
        profileLogger.addHandler(handler);
    }

    @AfterEach
    void tearDown() {
        profileLogger.removeHandler(handler);
        profileLogger.setLevel(previousLevel);
    }

    // ---- AC1: a bulk item failure is surfaced via the counter, not swallowed ----

    @Test
    void afterBulk_withItemError_incrementsFailedCountAndLogsWarning() {
        BulkResponseItem failedItem = item("bad-1", /* withError */ true);
        BulkResponse response = responseWithErrors(List.of(failedItem));

        listener.afterBulk(1L, emptyRequest(), Collections.emptyList(), response);

        assertEquals(1, totalFailed(), "item with non-null error() must be counted as failed");
        assertEquals(0, indexedCount.sum(), "a failed item must not be counted as indexed");
        assertTrue(loggedAtLeast(Level.WARNING, "Failed to index id=bad-1"),
                "the failure must be logged (surfaced), not swallowed");
    }

    @Test
    void afterBulk_successPath_incrementsIndexedCount() {
        // errors()==false: the listener fast-paths the whole batch as indexed.
        BulkResponse response = BulkResponse.of(b -> b
                .took(5)
                .errors(false)
                .items(List.of(item("ok-1", false), item("ok-2", false), item("ok-3", false))));

        listener.afterBulk(2L, emptyRequest(), Collections.emptyList(), response);

        assertEquals(3, indexedCount.sum());
        assertEquals(0, totalFailed());
    }

    @Test
    void afterBulk_mixedBatch_classifiesEachItemExactlyOnce() {
        // errors()==true with a mix: per-item branch counts failures and successes.
        List<BulkResponseItem> items = List.of(
                item("ok-1", false),
                item("bad-1", true),
                item("ok-2", false),
                item("bad-2", true));
        BulkResponse response = responseWithErrors(items);

        listener.afterBulk(3L, emptyRequest(), Collections.emptyList(), response);

        assertEquals(2, indexedCount.sum());
        assertEquals(2, totalFailed());
    }

    // ---- AC2: lossless accounting — emitted == indexed + failed ----

    @Test
    void losslessAccounting_indexedPlusFailedEqualsItemsSubmitted() {
        // A controlled "emitted" batch of 10, two of which fail at the item level.
        int emitted = 10;
        List<BulkResponseItem> items = new ArrayList<>();
        for (int i = 0; i < emitted; i++) {
            items.add(item("doc-" + i, /* fail items 3 and 7 */ i == 3 || i == 7));
        }
        BulkResponse response = responseWithErrors(items);

        listener.afterBulk(4L, emptyRequest(), Collections.emptyList(), response);

        long indexed = indexedCount.sum();
        long failed = totalFailed();
        assertEquals(8, indexed);
        assertEquals(2, failed);
        // The lossless invariant: every emitted/attempted doc is accounted for as
        // exactly one of indexed or failed — nothing silently disappears.
        assertEquals(emitted, indexed + failed, "emitted must equal indexed + failed");
    }

    // ---- AC3: an unrecoverable error is not mistaken for success ----

    @Test
    void afterBulk_wholeBatchNonRetryableThrowable_chargesGenuineFailuresAndLogsSevere() {
        // A non-retryable whole-batch error (a plain RuntimeException is NOT in the
        // retryable set) must NOT be retried (would loop) and is charged as genuine
        // data failures so a real structural break trips the strict guard.
        int batchSize = 7;
        BulkRequest request = requestWithOperations(batchSize);

        listener.afterBulk(5L, request, Collections.emptyList(),
                new RuntimeException("malformed request"));

        assertEquals(batchSize, failedPointsCount.sum(),
                "a points-targeted non-retryable whole-batch failure must land in failedPointsCount");
        assertEquals(0, transientPointsCharges.sum(), "non-retryable must not use the transient bucket");
        assertEquals(0, indexedCount.sum());
        assertTrue(loggedAtLeast(Level.SEVERE, "failed non-retryably"),
                "a non-retryable whole-batch error must be logged at SEVERE");
    }

    // ---- points-vs-bbox classification: bbox geo_shape rejects are warn-only ----

    @Test
    void bboxItemFailures_areCountedSeparately_andDoNotFailTheBuild() {
        // The real-world case: a few degenerate OSM relation geometries ES rejects with
        // "failed to parse field [bbox] of type [geo_shape]". They must be surfaced and
        // counted (lossless), but must NOT trip the build — they don't affect name search.
        List<BulkResponseItem> items = List.of(
                item(BBOX_INDEX, "rel-1", true),
                item(BBOX_INDEX, "rel-2", true),
                item(POINTS_INDEX, "ok-1", false));
        BulkResponse response = responseWithErrors(items);

        listener.afterBulk(7L, emptyRequest(), Collections.emptyList(), response);

        assertEquals(2, failedBboxCount.sum(), "bbox rejects must be counted in the bbox bucket");
        assertEquals(0, failedPointsCount.sum(), "no points doc failed");
        assertEquals(1, indexedCount.sum());
        assertEquals(2, totalFailed(), "lossless: bbox failures are still counted in the total");
        assertFalse(failsBuild(), "bbox-only failures must NOT fail the build (warn-only)");
    }

    /**
     * Mirrors the points half of PlanetSearchProfile.hasIndexingFailures() over the
     * two buckets, using the same thresholds. (The genuine-failure detail is covered
     * exhaustively by GuardThresholdsTest; here it just needs to stay false for the
     * small bbox-only fixture.)
     */
    private boolean failsBuild() {
        long emittedPoints = indexedCount.sum() + failedPointsCount.sum() + transientPointsCharges.sum();
        return failedPointsCount.sum() > IndexingStats.genuineFailureThreshold(emittedPoints)
                || transientPointsCharges.sum() > IndexingStats.transientChargeThreshold(emittedPoints);
    }

    /** Mirrors PlanetSearchProfile.getFailedCount(): points + bbox (the lossless total). */
    private long totalFailed() {
        return failedPointsCount.sum() + failedBboxCount.sum();
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private static BulkResponse responseWithErrors(List<BulkResponseItem> items) {
        return BulkResponse.of(b -> b.took(5).errors(true).items(items));
    }

    private static BulkResponseItem item(String id, boolean withError) {
        return item(POINTS_INDEX, id, withError);
    }

    private static BulkResponseItem item(String index, String id, boolean withError) {
        return BulkResponseItem.of(b -> {
            b.operationType(OperationType.Index).index(index).id(id).status(withError ? 400 : 200);
            if (withError) {
                b.error(e -> e.type("mapper_parsing_exception").reason("bad doc " + id));
            }
            return b;
        });
    }

    private static BulkRequest emptyRequest() {
        // The BulkResponse overload of afterBulk never reads the request; the 8.x
        // BulkRequest builder still requires a non-empty operations list, so supply one.
        return requestWithOperations(1, POINTS_INDEX);
    }

    private static BulkRequest requestWithOperations(int n) {
        return requestWithOperations(n, POINTS_INDEX);
    }

    private static BulkRequest requestWithOperations(int n, String index) {
        List<BulkOperation> ops = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            ops.add(BulkOperation.of(op -> op.index(ix -> ix.index(index).id("op-" + idx).document(Map.of("k", idx)))));
        }
        return BulkRequest.of(b -> b.operations(ops));
    }

    private boolean loggedAtLeast(Level level, String substring) {
        return handler.records.stream()
                .anyMatch(r -> r.getLevel() == level && formatted(r).contains(substring));
    }

    private static String formatted(LogRecord r) {
        // The production code logs via Supplier<String>; LogRecord.getMessage()
        // already resolves it for these java.util.logging calls.
        return r.getMessage() == null ? "" : r.getMessage();
    }

    private static final class RecordingHandler extends Handler {
        private final List<LogRecord> records = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void publish(LogRecord record) {
            records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
