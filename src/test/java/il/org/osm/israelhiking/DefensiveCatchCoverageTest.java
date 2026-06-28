package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;

import il.org.osm.israelhiking.ElasticsearchHelper.ElasticRunContext;

@Tag("unit")
class DefensiveCatchCoverageTest {

    private IndexingStats stats;
    private Logger profileLogger;
    private RecordingHandler profileHandler;
    private Level prevProfileLevel;

    @BeforeEach
    void setUp() {
        stats = new IndexingStats();
        profileLogger = Logger.getLogger(PlanetSearchProfile.class.getName());
        prevProfileLevel = profileLogger.getLevel();
        profileLogger.setLevel(Level.ALL);
        profileHandler = new RecordingHandler();
        profileLogger.addHandler(profileHandler);
    }

    @AfterEach
    void tearDown() {
        profileLogger.removeHandler(profileHandler);
        profileLogger.setLevel(prevProfileLevel);
    }

    @Test
    void addToIngester_whenEnqueueThrows_countsFailed_logsWarning_doesNotRethrow() {
        @SuppressWarnings("unchecked")
        BulkIngester<Void> ingester = mock(BulkIngester.class);
        doThrow(new IllegalStateException("Ingester is already closed"))
                .when(ingester).add(any(BulkOperation.class));

        ElasticRunContext context = new ElasticRunContext(
                null, "points", "bbox", "points1", "bbox1",
                new String[] { "he" }, ingester, null, stats);
        PlanetSearchProfile profile = new PlanetSearchProfile(null, context);

        BulkOperation op = BulkOperation.of(o -> o
                .index(ix -> ix.index("points1").id("doc-1").document(Map.of("k", 1))));

        assertDoesNotThrow(() -> profile.addToIngester(op, "doc-1"),
                "a closed/failed ingester must not abort the build");

        assertEquals(1, stats.getFailedPointsCount(), "a dropped points enqueue is charged to the points bucket");
        assertEquals(1, stats.getEmittedCount(), "the attempt is counted emitted before the enqueue is tried");
        assertTrue(loggedAtLeast(Level.WARNING, "Failed to enqueue document doc-1"),
                "the dropped document must be surfaced in the log, not swallowed silently");
    }

    @Test
    void listenerClose_whenIngesterCloseThrows_swallows_andStillDrainsRetries() {
        @SuppressWarnings("unchecked")
        BulkIngester<Void> ingester = mock(BulkIngester.class);
        doThrow(new IllegalStateException("close failed")).when(ingester).close();

        AccountingBulkListener listener = new AccountingBulkListener(null, stats, "bbox1");
        listener.attachIngester(ingester);

        assertDoesNotThrow(listener::close,
                "abort must not let a close() failure mask the original build error");

        verify(ingester).close();
        assertTrue(listener.isDrained(), "retries must still drain even when ingester close throws");
    }

    @Test
    void listenerClose_calledTwice_closesIngesterOnce_andDrains() {
        @SuppressWarnings("unchecked")
        BulkIngester<Void> ingester = mock(BulkIngester.class);
        AccountingBulkListener listener = new AccountingBulkListener(null, stats, "bbox1");
        listener.attachIngester(ingester);

        listener.close();
        listener.close();

        verify(ingester, times(1)).close();
        assertTrue(listener.isDrained(), "close() must drain retries");
    }

    private boolean loggedAtLeast(Level level, String substring) {
        return profileHandler.records.stream()
                .anyMatch(r -> r.getLevel() == level
                        && r.getMessage() != null && r.getMessage().contains(substring));
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
