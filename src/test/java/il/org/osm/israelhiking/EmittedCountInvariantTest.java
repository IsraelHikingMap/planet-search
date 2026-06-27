package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportOptions;

import il.org.osm.israelhiking.ElasticsearchHelper.ElasticRunContext;

@Tag("unit")
@ExtendWith(MockitoExtension.class)
class EmittedCountInvariantTest {

    private static final String POINTS_TARGET = "points1";
    private static final String BBOX_TARGET = "bbox1";

    @Test
    void eachInsertEmitsExactlyOnce_andEmittedEqualsIndexedPlusFailed() throws Exception {
        int points = 5;
        int bbox = 3;

        PlanetSearchProfile profile = newProfile();

        for (int i = 0; i < points; i++) {
            insertPoint(profile, "OSM_node_" + i);
        }
        for (int i = 0; i < bbox; i++) {
            insertBbox(profile);
        }

        assertEquals(points + bbox, profile.getEmittedCount(),
                "each successful insert must increment emittedCount exactly once");

        PlanetSearchProfile.AccountingBulkListener listener = listenerOver(profile);
        List<BulkResponseItem> items = new ArrayList<>();
        for (int i = 0; i < points; i++) {
            items.add(item(POINTS_TARGET, "OSM_node_" + i, i == 1));
        }
        for (int i = 0; i < bbox; i++) {
            items.add(item(BBOX_TARGET, "bbox-" + i, i == 0));
        }
        listener.afterBulk(1L, emptyRequest(), Collections.emptyList(),
                BulkResponse.of(b -> b.took(1).errors(true).items(items)));

        assertEquals(points + bbox, profile.getIndexedCount() + profile.getFailedCount(),
                "every emitted doc must be accounted for as exactly one of indexed/failed");
        assertEquals(profile.getEmittedCount(), profile.getIndexedCount() + profile.getFailedCount(),
                "lossless invariant: emitted == indexed + failed");
        assertEquals(1, profile.getFailedPointsCount(), "the failed points item lands in the points bucket");
        assertEquals(1, profile.getFailedBboxCount(), "the failed bbox item lands in the bbox bucket");
        assertTrue(profile.hasIndexingFailures(), "a dropped points doc must trip the fail-the-build gate");
    }

    private static PlanetSearchProfile newProfile() throws Exception {
        // ElasticRunContext is a final record, so it can't be Mockito-mocked; wrap a mock client in a real one.
        ElasticsearchTransport transport = mock(ElasticsearchTransport.class);
        when(transport.options()).thenReturn(mock(TransportOptions.class));
        // add() serializes the document eagerly, so it needs a real mapper.
        when(transport.jsonpMapper()).thenReturn(new JacksonJsonpMapper());
        ElasticsearchClient esClient = mock(ElasticsearchClient.class);
        when(esClient._transport()).thenReturn(transport);
        ElasticRunContext context = new ElasticRunContext(
                esClient, "points", "bbox", POINTS_TARGET, BBOX_TARGET, new String[] { "en" });

        Constructor<PlanetSearchProfile> ctor = PlanetSearchProfile.class.getDeclaredConstructor(
                com.onthegomap.planetiler.config.PlanetilerConfig.class, ElasticRunContext.class);
        ctor.setAccessible(true);
        return ctor.newInstance(null, context);
    }

    private static void insertPoint(PlanetSearchProfile profile, String docId) throws Exception {
        PointDocument doc = new PointDocument();
        doc.location = new double[] { 35.0, 31.0 };
        Method m = PlanetSearchProfile.class.getDeclaredMethod(
                "insertPointToElasticsearch", PointDocument.class, String.class);
        m.setAccessible(true);
        m.invoke(profile, doc, docId);
    }

    private static void insertBbox(PlanetSearchProfile profile) throws Exception {
        BBoxDocument bbox = new BBoxDocument();
        bbox.center = new double[] { 35.0, 31.0 };
        Method m = PlanetSearchProfile.class.getDeclaredMethod(
                "addBboxToBulk", BBoxDocument.class, String.class);
        m.setAccessible(true);
        m.invoke(profile, bbox, "bbox-" + System.nanoTime());
    }

    private static PlanetSearchProfile.AccountingBulkListener listenerOver(PlanetSearchProfile profile)
            throws Exception {
        return new PlanetSearchProfile.AccountingBulkListener(
                adder(profile, "indexedCount"),
                adder(profile, "failedPointsCount"),
                adder(profile, "failedBboxCount"),
                BBOX_TARGET);
    }

    private static LongAdder adder(PlanetSearchProfile profile, String name) throws Exception {
        Field f = PlanetSearchProfile.class.getDeclaredField(name);
        f.setAccessible(true);
        return (LongAdder) f.get(profile);
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
        return BulkRequest.of(b -> b.operations(
                op -> op.index(ix -> ix.index(POINTS_TARGET).id("op-0").document(Map.of("k", 0)))));
    }
}
