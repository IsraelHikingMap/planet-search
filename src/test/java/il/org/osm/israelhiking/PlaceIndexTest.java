package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;

@Tag("unit")
public class PlaceIndexTest {

    private static final String[] LANGUAGES = { "en", "he" };
    private static final GeometryFactory GF = new GeometryFactory();

    private static Map<String, Object> map(String... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static WithTags tags(String... kv) {
        return WithTags.from(map(kv));
    }

    /** A first-pass place node at (lon, lat). */
    private static OsmElement.Node node(long id, double lon, double lat, String... kv) {
        return new OsmElement.Node(id, map(kv), lat, lon);
    }

    /** A first-pass place relation with the given node members plus a way member (so it resolves to a polygon). */
    private static OsmElement.Relation placeRelation(long id, long[] memberNodeIds, String... kv) {
        var members = new ArrayList<OsmElement.Relation.Member>();
        members.add(new OsmElement.Relation.Member(OsmElement.Type.WAY, 999, "outer"));
        for (long nid : memberNodeIds) {
            members.add(new OsmElement.Relation.Member(OsmElement.Type.NODE, nid, "label"));
        }
        return new OsmElement.Relation(id, map(kv), members);
    }

    private static Geometry square(double minLon, double minLat, double maxLon, double maxLat) {
        return GF.createPolygon(new Coordinate[] {
                new Coordinate(minLon, minLat), new Coordinate(maxLon, minLat),
                new Coordinate(maxLon, maxLat), new Coordinate(minLon, maxLat),
                new Coordinate(minLon, minLat) });
    }

    /** A second-pass feature; geometry only matters for the WAY containment path. */
    private static SourceFeature feature(long id, Geometry geometry, String... kv) {
        return SimpleFeature.create(geometry, map(kv), id);
    }

    private static SourceFeature nodeFeature(long id, String... kv) {
        return feature(id, GF.createPoint(new Coordinate(0, 0)), kv);
    }

    @Test
    public void placeKeys_derivesNameAndWikidataKeys() {
        assertEquals(List.of("name=Afula", "wikidata=Q1"),
                PlaceIndex.placeKeys(tags("name", "Afula", "wikidata", "Q1")));
        assertEquals(List.of("name=Afula"), PlaceIndex.placeKeys(tags("name", "Afula")));
        assertEquals(List.of("wikidata=Q1"), PlaceIndex.placeKeys(tags("wikidata", "Q1")));
        assertEquals(List.of(), PlaceIndex.placeKeys(tags("place", "city")));
    }

    @Test
    public void estimatePopulation_emptyForNonPlaces() {
        assertTrue(PlaceIndex.estimatePopulation(tags("natural", "peak")).isEmpty());
        assertTrue(PlaceIndex.estimatePopulation(tags("place", "")).isEmpty());
    }

    @Test
    public void estimatePopulation_prefersTheTaggedValue() {
        assertEquals(83000, PlaceIndex.estimatePopulation(tags("place", "city", "population", "83000")).getAsInt());
    }

    @Test
    public void estimatePopulation_fallsBackToDefaultsByRank() {
        assertEquals(1_000_000, PlaceIndex.estimatePopulation(tags("place", "city")).getAsInt());
        assertEquals(50_000, PlaceIndex.estimatePopulation(tags("place", "town")).getAsInt());
        assertEquals(2_000, PlaceIndex.estimatePopulation(tags("place", "village")).getAsInt());
        assertEquals(200, PlaceIndex.estimatePopulation(tags("place", "hamlet")).getAsInt());
        assertEquals(20, PlaceIndex.estimatePopulation(tags("place", "isolated_dwelling")).getAsInt());
    }

    @Test
    public void shouldIndex_nodeYieldsToRelationItBelongsTo() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 5, 5, "place", "city", "name", "X"), LANGUAGES);
        index.recordRelation(placeRelation(100, new long[] { 1 }, "place", "city", "name", "X", "type", "boundary"));
        assertFalse(index.shouldIndex(nodeFeature(1, "name", "X")),
                "the node is a member of the same-named relation");
    }

    @Test
    public void shouldIndex_unrelatedSameNamedNodeSurvives() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 5, 5, "place", "city", "name", "X"), LANGUAGES);
        // A same-named relation elsewhere whose members do NOT include node 1.
        index.recordRelation(placeRelation(100, new long[] { 2 }, "place", "city", "name", "X", "type", "boundary"));
        assertTrue(index.shouldIndex(nodeFeature(1, "name", "X")),
                "same name, but not a member — a distinct place must survive");
    }

    @Test
    public void shouldIndex_memberOfADifferentlyNamedRelationSurvives() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 5, 5, "place", "city", "name", "X"), LANGUAGES);
        index.recordRelation(placeRelation(100, new long[] { 1 }, "place", "city", "name", "Y", "type", "boundary"));
        assertTrue(index.shouldIndex(nodeFeature(1, "name", "X")),
                "a member with a different name is a different place");
    }

    @Test
    public void shouldIndex_wayYieldsToNodeInsideIt() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 5, 5, "place", "town", "name", "X"), LANGUAGES);
        assertFalse(index.shouldIndex(feature(10, square(0, 0, 10, 10), "name", "X")),
                "the way contains the same-named node");
    }

    @Test
    public void shouldIndex_wayYieldsToNodeMatchedByWikidata() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 5, 5, "place", "town", "name", "Y", "wikidata", "Q1"), LANGUAGES);
        assertFalse(index.shouldIndex(feature(10, square(0, 0, 10, 10), "name", "X", "wikidata", "Q1")),
                "matched by wikidata, confirmed by containment even though the names differ");
    }

    @Test
    public void shouldIndex_wayWithSameNamedNodeElsewhereSurvives() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 50, 50, "place", "town", "name", "X"), LANGUAGES);
        assertTrue(index.shouldIndex(feature(10, square(0, 0, 10, 10), "name", "X")),
                "same name, but the node is outside the way — a distinct place must survive");
    }

    @Test
    public void shouldIndex_wayWithADifferentNamedNodeInsideSurvives() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 5, 5, "place", "town", "name", "Y"), LANGUAGES);
        assertTrue(index.shouldIndex(feature(10, square(0, 0, 10, 10), "name", "X")),
                "a differently-named node inside the way is a different place");
    }

    @Test
    public void shouldIndex_nonAreaWayIsIndexed() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 5, 5, "place", "town", "name", "X"), LANGUAGES);
        var openWay = feature(10, GF.createLineString(new Coordinate[] {
                new Coordinate(0, 0), new Coordinate(1, 1) }), "name", "X");
        assertTrue(index.shouldIndex(openWay), "a non-area way cannot contain a node");
    }
}
