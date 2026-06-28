package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.ExistsAliasRequest;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.BackoffPolicy;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportOptions;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.ObjectBuilder;

import il.org.osm.israelhiking.ElasticsearchHelper.ElasticRunContext;

@Tag("unit")
class FinalizeRunGateTest {

    private static final String POINTS_ALIAS = "points";
    private static final String BBOX_ALIAS = "bbox";
    private static final String POINTS_TARGET = "points1";
    private static final String BBOX_TARGET = "bbox1";

    @Test
    void cleanRun_swapsBothAliasesAndRestoresSearchSettings() throws Exception {
        ElasticsearchIndicesClient indices = mock(ElasticsearchIndicesClient.class);
        when(indices.existsAlias(anyExistsAlias())).thenReturn(new BooleanResponse(true));
        ElasticRunContext context = contextOver(indices);

        ElasticsearchHelper.finalizeRun(context);

        verify(indices).refresh(anyRefresh());
        verify(indices, times(2)).putSettings(anyPutSettings());
        verify(indices, times(2)).updateAliases(anyUpdateAliases());
        assertEquals(0, context.stats().getFailedPointsCount());
    }

    @Test
    void droppedPointsDoc_leavesPointsAliasOnPreviousIndex() throws Exception {
        ElasticsearchIndicesClient indices = mock(ElasticsearchIndicesClient.class);
        when(indices.existsAlias(anyExistsAlias())).thenReturn(new BooleanResponse(true));
        ElasticRunContext context = contextOver(indices);
        context.stats().failedPointsCount.increment();

        ElasticsearchHelper.finalizeRun(context);

        verify(indices, times(2)).putSettings(anyPutSettings());
        verify(indices, times(1)).updateAliases(anyUpdateAliases());
        assertTrue(context.stats().hasIndexingFailures());
    }

    @Test
    void droppedBboxOnly_stillSwapsBothAliases() throws Exception {
        ElasticsearchIndicesClient indices = mock(ElasticsearchIndicesClient.class);
        when(indices.existsAlias(anyExistsAlias())).thenReturn(new BooleanResponse(true));
        ElasticRunContext context = contextOver(indices);
        context.stats().failedBboxCount.increment();

        ElasticsearchHelper.finalizeRun(context);

        verify(indices, times(2)).updateAliases(anyUpdateAliases());
        assertTrue(!context.stats().hasIndexingFailures(), "a bbox-only drop must not hold back the points alias");
    }

    private static ElasticRunContext contextOver(ElasticsearchIndicesClient indices) {
        ElasticsearchTransport transport = mock(ElasticsearchTransport.class);
        when(transport.options()).thenReturn(mock(TransportOptions.class));
        when(transport.jsonpMapper()).thenReturn(new JacksonJsonpMapper());
        ElasticsearchClient esClient = mock(ElasticsearchClient.class);
        when(esClient._transport()).thenReturn(transport);
        when(esClient.indices()).thenReturn(indices);

        IndexingStats stats = new IndexingStats();
        AccountingBulkListener listener = new AccountingBulkListener(esClient, stats, BBOX_TARGET);
        BulkIngester<Void> ingester = BulkIngester.of(b -> b
                .client(esClient)
                .backoffPolicy(BackoffPolicy.noBackoff())
                .listener(listener));
        listener.attachIngester(ingester);
        return new ElasticRunContext(esClient, POINTS_ALIAS, BBOX_ALIAS, POINTS_TARGET, BBOX_TARGET,
                new String[] { "en" }, ingester, listener, stats);
    }

    private static Function<ExistsAliasRequest.Builder, ObjectBuilder<ExistsAliasRequest>> anyExistsAlias() {
        return ArgumentMatchers.any();
    }

    private static Function<RefreshRequest.Builder, ObjectBuilder<RefreshRequest>> anyRefresh() {
        return ArgumentMatchers.any();
    }

    private static Function<PutIndicesSettingsRequest.Builder, ObjectBuilder<PutIndicesSettingsRequest>> anyPutSettings() {
        return ArgumentMatchers.any();
    }

    private static Function<UpdateAliasesRequest.Builder, ObjectBuilder<UpdateAliasesRequest>> anyUpdateAliases() {
        return ArgumentMatchers.any();
    }
}
