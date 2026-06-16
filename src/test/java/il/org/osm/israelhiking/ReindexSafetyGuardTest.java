package il.org.osm.israelhiking;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.ExistsAliasRequest;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.ObjectBuilder;

import java.io.IOException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * Guards the root fix for the incident where a stray "docker compose up site"
 * pulled the indexer (default area=us/colorado) and reindexed OVER the live
 * whole-planet index, dropping it from 22.8M docs to ~83k.
 *
 * The guard checks the LIVE (currently-aliased) doc-count — NOT the inactive
 * target index the build writes to — so a legitimate alias-rotation rebuild still
 * works once it passes --force-reindex.
 */
@Tag("unit")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReindexSafetyGuardTest {

    @Mock
    private ElasticsearchClient esClient;

    @Mock
    private ElasticsearchIndicesClient indicesClient;

    private final String alias = "points";

    /** Make the alias exist and resolve to liveDocs live documents. */
    private void stubLiveCount(long liveDocs) throws IOException {
        when(esClient.indices()).thenReturn(indicesClient);
        when(indicesClient.existsAlias(
                ArgumentMatchers.<Function<ExistsAliasRequest.Builder, ObjectBuilder<ExistsAliasRequest>>>any()))
                .thenReturn(new BooleanResponse(true));
        when(esClient.count(
                ArgumentMatchers.<Function<CountRequest.Builder, ObjectBuilder<CountRequest>>>any()))
                .thenReturn(CountResponse.of(b -> b.count(liveDocs)
                        .shards(s -> s.total(1).successful(1).failed(0))));
    }

    @Test
    void populatedLiveIndexWithoutForce_blocksAndDoesNotDelete() throws Exception {
        stubLiveCount(22_843_494L);

        var ex = assertThrows(IllegalStateException.class,
                () -> ElasticsearchHelper.assertSafeToReindex(esClient, alias, 1_000_000L, false));
        // The error must name the live count so operators understand what was protected.
        org.junit.jupiter.api.Assertions.assertTrue(ex.getMessage().contains("22843494"));
        // Nothing destructive must have been attempted.
        verify(indicesClient, never()).delete(
                ArgumentMatchers.<Function<co.elastic.clients.elasticsearch.indices.DeleteIndexRequest.Builder,
                        ObjectBuilder<co.elastic.clients.elasticsearch.indices.DeleteIndexRequest>>>any());
    }

    @Test
    void populatedLiveIndexWithForce_isAllowed() throws Exception {
        stubLiveCount(22_843_494L);

        assertDoesNotThrow(
                () -> ElasticsearchHelper.assertSafeToReindex(esClient, alias, 1_000_000L, true));
    }

    @Test
    void liveCountBelowThreshold_isAllowed() throws Exception {
        stubLiveCount(82_938L);

        assertDoesNotThrow(
                () -> ElasticsearchHelper.assertSafeToReindex(esClient, alias, 1_000_000L, false));
    }

    @Test
    void missingAlias_countsAsZero_andAllows() throws Exception {
        when(esClient.indices()).thenReturn(indicesClient);
        when(indicesClient.existsAlias(
                ArgumentMatchers.<Function<ExistsAliasRequest.Builder, ObjectBuilder<ExistsAliasRequest>>>any()))
                .thenReturn(new BooleanResponse(false));

        assertEquals(0L, ElasticsearchHelper.getLiveAliasDocCount(esClient, alias));
        assertDoesNotThrow(
                () -> ElasticsearchHelper.assertSafeToReindex(esClient, alias, 1_000_000L, false));
    }

    @Test
    void unreachableCluster_failsOpenToZero_andAllows() throws Exception {
        when(esClient.indices()).thenReturn(indicesClient);
        when(indicesClient.existsAlias(
                ArgumentMatchers.<Function<ExistsAliasRequest.Builder, ObjectBuilder<ExistsAliasRequest>>>any()))
                .thenThrow(new RuntimeException("connection refused"));

        assertEquals(0L, ElasticsearchHelper.getLiveAliasDocCount(esClient, alias));
        assertDoesNotThrow(
                () -> ElasticsearchHelper.assertSafeToReindex(esClient, alias, 1_000_000L, false));
    }

    /**
     * The alias EXISTS but its doc-count read fails. This MUST NOT be read as "0 docs" — a transient
     * count error could otherwise defeat the guard and wipe a populated live index. Fail CLOSED.
     */
    @Test
    void existingAliasCountReadFails_failsClosed_andBlocks() throws Exception {
        when(esClient.indices()).thenReturn(indicesClient);
        when(indicesClient.existsAlias(
                ArgumentMatchers.<Function<ExistsAliasRequest.Builder, ObjectBuilder<ExistsAliasRequest>>>any()))
                .thenReturn(new BooleanResponse(true));
        when(esClient.count(
                ArgumentMatchers.<Function<CountRequest.Builder, ObjectBuilder<CountRequest>>>any()))
                .thenThrow(new RuntimeException("read timeout"));

        // getLiveAliasDocCount must throw (NOT return 0) when a present alias can't be counted.
        assertThrows(IllegalStateException.class,
                () -> ElasticsearchHelper.getLiveAliasDocCount(esClient, alias));
        // ...so the guard blocks the reindex instead of silently allowing a wipe.
        assertThrows(IllegalStateException.class,
                () -> ElasticsearchHelper.assertSafeToReindex(esClient, alias, 1_000_000L, false));
    }

    /**
     * A forced rebuild bypasses the guard BEFORE the count is read, so a count read-error must not
     * block a legitimate `make prod` force build.
     */
    @Test
    void existingAliasCountReadFails_withForce_isAllowed() throws Exception {
        when(esClient.indices()).thenReturn(indicesClient);
        when(indicesClient.existsAlias(
                ArgumentMatchers.<Function<ExistsAliasRequest.Builder, ObjectBuilder<ExistsAliasRequest>>>any()))
                .thenReturn(new BooleanResponse(true));
        when(esClient.count(
                ArgumentMatchers.<Function<CountRequest.Builder, ObjectBuilder<CountRequest>>>any()))
                .thenThrow(new RuntimeException("read timeout"));

        assertDoesNotThrow(
                () -> ElasticsearchHelper.assertSafeToReindex(esClient, alias, 1_000_000L, true));
    }
}
