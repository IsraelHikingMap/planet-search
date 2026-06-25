package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

        assertEquals(1, stats.failedCount.sum(), "a dropped enqueue must be charged failed");
        assertEquals(0, stats.emittedCount.sum(), "a failed enqueue must not be counted emitted");
        assertTrue(loggedAtLeast(Level.WARNING, "Failed to enqueue document doc-1"),
                "the dropped document must be surfaced in the log, not swallowed silently");
    }

    @Test
    void contextClose_whenIngesterCloseThrows_swallows_andStillAwaitsRetries() {
        @SuppressWarnings("unchecked")
        BulkIngester<Void> ingester = mock(BulkIngester.class);
        doThrow(new IllegalStateException("close failed")).when(ingester).close();

        AccountingBulkListener listener = mock(AccountingBulkListener.class);
        when(listener.tryClaimIngesterClose()).thenReturn(true);

        ElasticRunContext context = new ElasticRunContext(
                null, "points", "bbox", "points1", "bbox1",
                new String[] { "he" }, ingester, listener, stats);

        assertDoesNotThrow(context::close,
                "abort must not let a close() failure mask the original build error");

        verify(ingester).close();
        verify(listener).awaitRetries();
    }

    @Test
    void finalizeThenAutoClose_doesNotCloseIngesterTwice() {
        @SuppressWarnings("unchecked")
        BulkIngester<Void> ingester = mock(BulkIngester.class);
        AccountingBulkListener listener = new AccountingBulkListener(null, stats);

        ElasticRunContext context = new ElasticRunContext(
                null, "points", "bbox", "points1", "bbox1",
                new String[] { "he" }, ingester, listener, stats);

        assertTrue(listener.tryClaimIngesterClose(), "finalizeRun claims the close first");
        context.close();

        assertFalse(listener.tryClaimIngesterClose(), "the claim is one-shot across finalizeRun + close()");
        verify(ingester, times(0)).close();
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
