package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    /** A first-pass place relation that resolves to a polygon (a boundary with a way member). */
    private static OsmElement.Relation placeRelation(long id, String wikidata) {
        var members = List.of(new OsmElement.Relation.Member(OsmElement.Type.WAY, 999, "outer"));
        return new OsmElement.Relation(id,
                map("place", "town", "type", "boundary", "name", "R", "wikidata", wikidata), members);
    }

    private static Geometry square(double minLon, double minLat, double maxLon, double maxLat) {
        return GF.createPolygon(new Coordinate[] {
                new Coordinate(minLon, minLat), new Coordinate(maxLon, minLat),
                new Coordinate(maxLon, maxLat), new Coordinate(minLon, maxLat),
                new Coordinate(minLon, minLat) });
    }

    /** A second-pass feature; geometry drives whether it is treated as a point or a polygon. */
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
    public void shouldIndex_nodeWithoutAMatchingRelationIsIndexed() throws Exception {
        var index = new PlaceIndex();
        assertTrue(index.shouldIndex(nodeFeature(1, "name", "X")), "no wikidata, nothing to yield to");
        index.recordRelation(placeRelation(100, "Q1"));
        assertTrue(index.shouldIndex(nodeFeature(1, "name", "X", "wikidata", "Q2")),
                "a different wikidata than any place relation — the node stands on its own");
    }

    @Test
    public void shouldIndex_nodeYieldsToRelationSharingItsWikidata() throws Exception {
        var index = new PlaceIndex();
        index.recordRelation(placeRelation(100, "Q1"));
        assertFalse(index.shouldIndex(nodeFeature(1, "name", "X", "wikidata", "Q1")),
                "the relation carrying this wikidata wins");
    }

    @Test
    public void shouldIndex_polygonYieldsToNodeInsideIt() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 5, 5, "place", "town", "name", "X"), LANGUAGES);
        assertFalse(index.shouldIndex(feature(10, square(0, 0, 10, 10), "name", "X")),
                "the polygon contains the same-named node");
    }

    @Test
    public void shouldIndex_polygonYieldsToNodeMatchedByWikidata() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 5, 5, "place", "town", "name", "Y", "wikidata", "Q1"), LANGUAGES);
        assertFalse(index.shouldIndex(feature(10, square(0, 0, 10, 10), "name", "X", "wikidata", "Q1")),
                "matched by wikidata, confirmed by containment even though the names differ");
    }

    @Test
    public void shouldIndex_polygonWithSameNamedNodeElsewhereSurvives() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 50, 50, "place", "town", "name", "X"), LANGUAGES);
        assertTrue(index.shouldIndex(feature(10, square(0, 0, 10, 10), "name", "X")),
                "same name, but the node is outside the polygon — a distinct place must survive");
    }

    @Test
    public void shouldIndex_polygonWithADifferentNamedNodeInsideSurvives() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 5, 5, "place", "town", "name", "Y"), LANGUAGES);
        assertTrue(index.shouldIndex(feature(10, square(0, 0, 10, 10), "name", "X")),
                "a differently-named node inside the polygon is a different place");
    }

    @Test
    public void shouldIndex_nonAreaPolygonIsIndexed() throws Exception {
        var index = new PlaceIndex();
        index.recordNode(node(1, 5, 5, "place", "town", "name", "X"), LANGUAGES);
        var openWay = feature(10, GF.createLineString(new Coordinate[] {
                new Coordinate(0, 0), new Coordinate(1, 1) }), "name", "X");
        assertTrue(index.shouldIndex(openWay), "a non-area way cannot contain a node");
    }
}
