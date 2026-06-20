package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;

@Tag("unit")
class BulkIngesterAccountingTest {

    private static final String POINTS_INDEX = "points";

    private IndexingStats stats;
    private LongAdder indexedCount;
    private LongAdder failedCount;
    private AccountingBulkListener listener;

    private Logger profileLogger;
    private RecordingHandler handler;
    private Level previousLevel;

    @BeforeEach
    void setUp() {
        stats = new IndexingStats();
        indexedCount = stats.indexedCount;
        failedCount = stats.failedCount;
        listener = new AccountingBulkListener(null, stats);

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

        assertEquals(1, failedCount.sum(), "item with non-null error() must be counted as failed");
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

        listener.afterBulk(2L, requestWithOperations(3), Collections.emptyList(), response);

        assertEquals(3, indexedCount.sum());
        assertEquals(0, failedCount.sum());
    }

    @Test
    void afterBulk_mixedBatch_classifiesEachItemExactlyOnce() {
        List<BulkResponseItem> items = List.of(
                item("ok-1", false),
                item("bad-1", true),
                item("ok-2", false),
                item("bad-2", true));
        BulkResponse response = responseWithErrors(items);

        listener.afterBulk(3L, requestWithOperations(items.size()), Collections.emptyList(), response);

        assertEquals(2, indexedCount.sum());
        assertEquals(2, failedCount.sum());
    }

    @Test
    void losslessAccounting_indexedPlusFailedEqualsItemsSubmitted() {
        int emitted = 10;
        List<BulkResponseItem> items = new ArrayList<>();
        for (int i = 0; i < emitted; i++) {
            items.add(item("doc-" + i, i == 3 || i == 7));
        }
        BulkResponse response = responseWithErrors(items);

        listener.afterBulk(4L, requestWithOperations(emitted), Collections.emptyList(), response);

        long indexed = indexedCount.sum();
        long failed = failedCount.sum();
        assertEquals(8, indexed);
        assertEquals(2, failed);
        assertEquals(emitted, indexed + failed, "emitted must equal indexed + failed");
    }

    @Test
    void afterBulk_wholeBatchNonRetryableThrowable_countsFailedAndLogsSevere() {
        int batchSize = 7;
        BulkRequest request = requestWithOperations(batchSize);

        listener.afterBulk(5L, request, Collections.emptyList(),
                new RuntimeException("malformed request"));

        assertEquals(batchSize, failedCount.sum(),
                "a non-retryable whole-batch failure must count every op as failed");
        assertEquals(0, indexedCount.sum());
        assertTrue(loggedAtLeast(Level.SEVERE, "failed non-retryably"),
                "a non-retryable whole-batch error must be logged at SEVERE");
    }

    @Test
    void indexingStats_gettersSumTheUnderlyingAdders() {
        IndexingStats s = new IndexingStats();
        assertEquals(0, s.getIndexedCount());
        assertEquals(0, s.getFailedCount());
        assertEquals(0, s.getEmittedCount());

        s.indexedCount.add(7);
        s.failedCount.add(3);
        s.emittedCount.add(10);

        assertEquals(7, s.getIndexedCount());
        assertEquals(3, s.getFailedCount());
        assertEquals(10, s.getEmittedCount(), "emitted is tracked independently of indexed/failed");
    }

    @Test
    void afterBulk_nonRetryableThrowableWithNullMessage_stillCountsFailedAndDescribes() {
        // A throwable with a null message exercises the null-message arm of describe(...)
        // without an NPE; ops must still all be charged failed.
        listener.afterBulk(9L, requestWithOperations(2), Collections.emptyList(),
                new RuntimeException());

        assertEquals(2, failedCount.sum());
        assertEquals(0, indexedCount.sum());
        assertTrue(loggedAtLeast(Level.SEVERE, "RuntimeException"),
                "describe(...) must name the throwable class even when its message is null");
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
        // The BulkRequest builder requires a non-empty operations list.
        return requestWithOperations(1);
    }

    private static BulkRequest requestWithOperations(int n) {
        List<BulkOperation> ops = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            ops.add(BulkOperation.of(op -> op.index(ix -> ix.index(POINTS_INDEX).id("op-" + idx).document(Map.of("k", idx)))));
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
