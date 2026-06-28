package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;

@Tag("unit")
class BulkIngesterAccountingTest {

    private static final String POINTS_INDEX = "points";
    private static final String BBOX_INDEX = "bbox";

    private IndexingStats stats;
    private AccountingBulkListener listener;

    private Logger listenerLogger;
    private RecordingHandler handler;
    private Level previousLevel;

    @BeforeEach
    void setUp() {
        stats = new IndexingStats();
        listener = new AccountingBulkListener(null, stats, BBOX_INDEX);

        listenerLogger = Logger.getLogger(AccountingBulkListener.class.getName());
        previousLevel = listenerLogger.getLevel();
        listenerLogger.setLevel(Level.ALL);
        handler = new RecordingHandler();
        listenerLogger.addHandler(handler);
    }

    @AfterEach
    void tearDown() {
        listenerLogger.removeHandler(handler);
        listenerLogger.setLevel(previousLevel);
    }

    @Test
    void afterBulk_withItemError_incrementsFailedPointsAndLogsWarning() {
        BulkResponse response = responseWithErrors(List.of(item(POINTS_INDEX, "bad-1", true)));

        listener.afterBulk(1L, requestWithOperations(1, POINTS_INDEX), Collections.emptyList(), response);

        assertEquals(1, stats.getFailedPointsCount(), "a points item with non-null error() lands in the points bucket");
        assertEquals(0, stats.getIndexedCount(), "a failed item must not be counted as indexed");
        assertTrue(loggedAtLeast(Level.WARNING, "Failed to index id=bad-1"),
                "the failure must be logged (surfaced), not swallowed");
    }

    @Test
    void afterBulk_successPath_incrementsIndexedCount() {
        BulkResponse response = BulkResponse.of(b -> b
                .took(5)
                .errors(false)
                .items(List.of(item(POINTS_INDEX, "ok-1", false), item(POINTS_INDEX, "ok-2", false),
                        item(POINTS_INDEX, "ok-3", false))));

        listener.afterBulk(2L, requestWithOperations(3, POINTS_INDEX), Collections.emptyList(), response);

        assertEquals(3, stats.getIndexedCount());
        assertEquals(0, stats.getFailedCount());
    }

    @Test
    void afterBulk_mixedBatch_classifiesEachItemExactlyOnce() {
        List<BulkResponseItem> items = List.of(
                item(POINTS_INDEX, "ok-1", false),
                item(POINTS_INDEX, "bad-1", true),
                item(POINTS_INDEX, "ok-2", false),
                item(POINTS_INDEX, "bad-2", true));
        BulkResponse response = responseWithErrors(items);

        listener.afterBulk(3L, requestWithOperations(items.size(), POINTS_INDEX), Collections.emptyList(), response);

        assertEquals(2, stats.getIndexedCount());
        assertEquals(2, stats.getFailedCount());
    }

    @Test
    void losslessAccounting_indexedPlusFailedEqualsItemsSubmitted() {
        int emitted = 10;
        List<BulkResponseItem> items = new ArrayList<>();
        for (int i = 0; i < emitted; i++) {
            items.add(item(POINTS_INDEX, "doc-" + i, i == 3 || i == 7));
        }
        BulkResponse response = responseWithErrors(items);

        listener.afterBulk(4L, requestWithOperations(emitted, POINTS_INDEX), Collections.emptyList(), response);

        long indexed = stats.getIndexedCount();
        long failed = stats.getFailedCount();
        assertEquals(8, indexed);
        assertEquals(2, failed);
        assertEquals(emitted, indexed + failed, "emitted must equal indexed + failed");
    }

    @Test
    void bboxItemFailures_areCountedSeparately_andDoNotTripTheBuildGate() {
        List<BulkResponseItem> items = List.of(
                item(BBOX_INDEX, "rel-1", true),
                item(BBOX_INDEX, "rel-2", true),
                item(POINTS_INDEX, "ok-1", false));
        BulkResponse response = responseWithErrors(items);

        listener.afterBulk(7L, requestWithOperations(items.size(), POINTS_INDEX), Collections.emptyList(), response);

        assertEquals(2, stats.getFailedBboxCount(), "bbox rejects must be counted in the bbox bucket");
        assertEquals(0, stats.getFailedPointsCount(), "no points doc failed");
        assertEquals(1, stats.getIndexedCount());
        assertEquals(2, stats.getFailedCount(), "lossless: bbox failures are still counted in the total");
        assertTrue(!stats.hasIndexingFailures(), "bbox-only failures must NOT trip the build-fail gate");
    }

    @Test
    void unknownDestination_isFailClosedAsPoints() {
        BulkResponse response = responseWithErrors(List.of(item("mystery", "x-1", true)));

        listener.afterBulk(8L, requestWithOperations(1, POINTS_INDEX), Collections.emptyList(), response);

        assertEquals(1, stats.getFailedPointsCount(), "an unknown destination must fail closed as points");
        assertEquals(0, stats.getFailedBboxCount());
        assertTrue(stats.hasIndexingFailures());
    }

    @Test
    void wholeBatchNonRetryableThrowable_countsPointsFailed_andLogsSevere() {
        int batchSize = 7;
        BulkRequest request = requestWithOperations(batchSize, POINTS_INDEX);

        listener.afterBulk(5L, request, Collections.emptyList(), new RuntimeException("malformed request"));

        assertEquals(batchSize, stats.getFailedPointsCount(),
                "a non-retryable points whole-batch failure lands wholly in the points bucket");
        assertEquals(0, stats.getIndexedCount());
        assertTrue(loggedAtLeast(Level.SEVERE, "failed non-retryably"),
                "a non-retryable whole-batch error must be logged at SEVERE");
    }

    @Test
    void wholeBatchNonRetryableThrowable_classifiesByOperationDestination() {
        List<BulkOperation> ops = List.of(
                BulkOperation.of(op -> op.index(ix -> ix.index(BBOX_INDEX).id("b-1").document(Map.of("k", 1)))),
                BulkOperation.of(op -> op.index(ix -> ix.index(POINTS_INDEX).id("p-1").document(Map.of("k", 2)))),
                BulkOperation.of(op -> op.index(ix -> ix.index(POINTS_INDEX).id("p-2").document(Map.of("k", 3)))));
        BulkRequest request = BulkRequest.of(b -> b.operations(ops));

        listener.afterBulk(6L, request, Collections.emptyList(), new RuntimeException("connection refused permanently"));

        assertEquals(1, stats.getFailedBboxCount());
        assertEquals(2, stats.getFailedPointsCount());
    }

    @Test
    void indexingStats_gettersSumTheUnderlyingAdders() {
        IndexingStats s = new IndexingStats();
        assertEquals(0, s.getIndexedCount());
        assertEquals(0, s.getFailedCount());
        assertEquals(0, s.getEmittedCount());

        s.indexedCount.add(7);
        s.failedPointsCount.add(2);
        s.failedBboxCount.add(1);
        s.emittedCount.add(10);

        assertEquals(7, s.getIndexedCount());
        assertEquals(3, s.getFailedCount(), "failed total is points + bbox");
        assertEquals(2, s.getFailedPointsCount());
        assertEquals(1, s.getFailedBboxCount());
        assertEquals(10, s.getEmittedCount(), "emitted is tracked independently of indexed/failed");
        assertTrue(s.hasIndexingFailures(), "a points failure trips the build-fail gate");
    }

    @Test
    void afterBulk_nonRetryableThrowableWithNullMessage_stillCountsFailedAndDescribes() {
        listener.afterBulk(9L, requestWithOperations(2, POINTS_INDEX), Collections.emptyList(), new RuntimeException());

        assertEquals(2, stats.getFailedPointsCount());
        assertEquals(0, stats.getIndexedCount());
        assertTrue(loggedAtLeast(Level.SEVERE, "RuntimeException"),
                "describe(...) must name the throwable class even when its message is null");
    }

    private static BulkResponse responseWithErrors(List<BulkResponseItem> items) {
        return BulkResponse.of(b -> b.took(5).errors(true).items(items));
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
