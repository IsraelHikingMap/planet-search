package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.ExistsAliasRequest;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportOptions;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.ObjectBuilder;

import il.org.osm.israelhiking.ElasticsearchHelper.ElasticRunContext;

@Tag("unit")
class IndexerResilienceTest {

    private static final String POINTS_ALIAS = "points";
    private static final String BBOX_ALIAS = "bbox";
    private static final String POINTS_TARGET = "points1";
    private static final String BBOX_TARGET = "bbox1";

    @Test
    void finalizeRun_cleanRun_swapsBothAliasesAndRestoresSearchSettings() throws Exception {
        ElasticsearchIndicesClient indices = mock(ElasticsearchIndicesClient.class);
        when(indices.existsAlias(anyExistsAlias())).thenReturn(new BooleanResponse(true));
        ElasticRunContext context = contextOver(indices);
        PlanetSearchProfile profile = newProfile(context);

        ElasticsearchHelper.finalizeRun(context, profile);

        verify(indices).refresh(anyRefresh());
        verify(indices, times(2)).putSettings(anyPutSettings());
        verify(indices, times(2)).updateAliases(anyUpdateAliases());
        assertEquals(0, profile.getFailedPointsCount());
    }

    @Test
    void finalizeRun_droppedPointsDoc_leavesPointsAliasOnPreviousIndex() throws Exception {
        ElasticsearchIndicesClient indices = mock(ElasticsearchIndicesClient.class);
        when(indices.existsAlias(anyExistsAlias())).thenReturn(new BooleanResponse(true));
        ElasticRunContext context = contextOver(indices);
        PlanetSearchProfile profile = newProfile(context);
        adder(profile, "failedPointsCount").increment();

        ElasticsearchHelper.finalizeRun(context, profile);

        verify(indices, times(2)).putSettings(anyPutSettings());
        verify(indices, times(1)).updateAliases(anyUpdateAliases());
        assertTrue(profile.hasIndexingFailures());
    }

    @Test
    void enqueueAfterClose_isCountedAsFailed_neverSwallowed() throws Exception {
        ElasticRunContext context = contextOver(mock(ElasticsearchIndicesClient.class));
        PlanetSearchProfile profile = newProfile(context);
        profile.flush();

        invokeInsertPoint(profile, "OSM_node_after_close");
        invokeAddBbox(profile, "bbox_after_close");

        assertEquals(0, profile.getEmittedCount(), "a failed enqueue must not be counted as emitted");
        assertEquals(1, profile.getFailedPointsCount(), "a points enqueue failure lands in the points bucket");
        assertEquals(1, profile.getFailedBboxCount(), "a bbox enqueue failure lands in the bbox bucket");
        assertTrue(profile.hasIndexingFailures(), "a dropped points doc must trip the fail-the-build gate");
    }

    private static ElasticRunContext contextOver(ElasticsearchIndicesClient indices) {
        ElasticsearchTransport transport = mock(ElasticsearchTransport.class);
        when(transport.options()).thenReturn(mock(TransportOptions.class));
        when(transport.jsonpMapper()).thenReturn(new JacksonJsonpMapper());
        ElasticsearchClient esClient = mock(ElasticsearchClient.class);
        when(esClient._transport()).thenReturn(transport);
        when(esClient.indices()).thenReturn(indices);
        return new ElasticRunContext(esClient, POINTS_ALIAS, BBOX_ALIAS, POINTS_TARGET, BBOX_TARGET,
                new String[] { "en" });
    }

    private static PlanetSearchProfile newProfile(ElasticRunContext context) throws Exception {
        Constructor<PlanetSearchProfile> ctor = PlanetSearchProfile.class.getDeclaredConstructor(
                com.onthegomap.planetiler.config.PlanetilerConfig.class, ElasticRunContext.class);
        ctor.setAccessible(true);
        return ctor.newInstance(null, context);
    }

    private static void invokeInsertPoint(PlanetSearchProfile profile, String docId) throws Exception {
        PointDocument doc = new PointDocument();
        doc.location = new double[] { 35.0, 31.0 };
        Method m = PlanetSearchProfile.class.getDeclaredMethod(
                "insertPointToElasticsearch", PointDocument.class, String.class);
        m.setAccessible(true);
        m.invoke(profile, doc, docId);
    }

    private static void invokeAddBbox(PlanetSearchProfile profile, String docId) throws Exception {
        BBoxDocument bbox = new BBoxDocument();
        bbox.center = new double[] { 35.0, 31.0 };
        Method m = PlanetSearchProfile.class.getDeclaredMethod(
                "addBboxToBulk", BBoxDocument.class, String.class);
        m.setAccessible(true);
        m.invoke(profile, bbox, docId);
    }

    private static LongAdder adder(PlanetSearchProfile profile, String name) throws Exception {
        Field f = PlanetSearchProfile.class.getDeclaredField(name);
        f.setAccessible(true);
        return (LongAdder) f.get(profile);
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
