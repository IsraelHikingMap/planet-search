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
import org.mockito.junit.jupiter.MockitoExtension;

import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;

@Tag("unit")
@ExtendWith(MockitoExtension.class)
class BulkIngesterAccountingTest {

    private static final String POINTS_INDEX = "points";
    private static final String BBOX_INDEX = "bbox";

    private LongAdder indexedCount;
    private LongAdder failedPointsCount;
    private LongAdder failedBboxCount;
    private PlanetSearchProfile.AccountingBulkListener listener;

    private Logger profileLogger;
    private RecordingHandler handler;
    private Level previousLevel;

    @BeforeEach
    void setUp() {
        indexedCount = new LongAdder();
        failedPointsCount = new LongAdder();
        failedBboxCount = new LongAdder();
        listener = new PlanetSearchProfile.AccountingBulkListener(
                indexedCount, failedPointsCount, failedBboxCount, BBOX_INDEX);

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

    @Test
    void afterBulk_withItemError_incrementsFailedCountAndLogsWarning() {
        BulkResponseItem failedItem = item("bad-1", true);
        BulkResponse response = responseWithErrors(List.of(failedItem));

        listener.afterBulk(1L, emptyRequest(), Collections.emptyList(), response);

        assertEquals(1, totalFailed(), "item with non-null error() must be counted as failed");
        assertEquals(0, indexedCount.sum(), "a failed item must not be counted as indexed");
        assertTrue(loggedAtLeast(Level.WARNING, "Failed to index id=bad-1"),
                "the failure must be logged (surfaced), not swallowed");
    }

    @Test
    void afterBulk_successPath_incrementsIndexedCount() {
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

    @Test
    void losslessAccounting_indexedPlusFailedEqualsItemsSubmitted() {
        int emitted = 10;
        List<BulkResponseItem> items = new ArrayList<>();
        for (int i = 0; i < emitted; i++) {
            items.add(item("doc-" + i, i == 3 || i == 7));
        }
        BulkResponse response = responseWithErrors(items);

        listener.afterBulk(4L, emptyRequest(), Collections.emptyList(), response);

        long indexed = indexedCount.sum();
        long failed = totalFailed();
        assertEquals(8, indexed);
        assertEquals(2, failed);
        assertEquals(emitted, indexed + failed, "emitted must equal indexed + failed");
    }

    @Test
    void afterBulk_wholeBatchThrowable_addsBatchSizeToFailedAndLogsSevere() {
        int batchSize = 7;
        BulkRequest request = requestWithOperations(batchSize);

        listener.afterBulk(5L, request, Collections.emptyList(),
                new RuntimeException("connection reset"));

        assertEquals(batchSize, totalFailed(),
                "whole-batch failure must add the entire request size to failedCount");
        assertEquals(batchSize, failedPointsCount.sum(),
                "a points-targeted whole-batch failure must land in failedPointsCount");
        assertEquals(0, indexedCount.sum());
        assertTrue(loggedAtLeast(Level.SEVERE, "failed entirely"),
                "an unrecoverable whole-batch error must be logged at SEVERE");
    }

    @Test
    void failBuildDecision_isTakenWhenPointsDocFailed_andNotOnCleanRun() {
        assertFalse(failsBuild(), "a clean run (no failures) must NOT fail the build");

        listener.afterBulk(6L, requestWithOperations(3, POINTS_INDEX), Collections.emptyList(),
                new RuntimeException("boom"));

        assertTrue(failsBuild(),
                "once any POINTS document fails, the build must fail (no partial index mistaken for success)");
    }

    @Test
    void bboxItemFailures_areCountedSeparately_andDoNotFailTheBuild() {
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

    /** Mirrors PlanetSearchProfile.hasIndexingFailures(): failedPointsCount.sum() > 0. */
    private boolean failsBuild() {
        return failedPointsCount.sum() > 0;
    }

    /** Mirrors PlanetSearchProfile.getFailedCount(): points + bbox (the lossless total). */
    private long totalFailed() {
        return failedPointsCount.sum() + failedBboxCount.sum();
    }

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
        // The 8.x BulkRequest builder rejects an empty operations list, so supply one even though this overload never reads it.
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
