package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;

import il.org.osm.israelhiking.PlaceIndex.Kind;

@Tag("unit")
public class PlaceIndexTest {

    // ---- fixtures ----

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

    private static OsmElement.Node node(String... kv) {
        return new OsmElement.Node(1, map(kv), 0, 0);
    }

    private static OsmElement.Relation relation(List<OsmElement.Relation.Member> members, String... kv) {
        return new OsmElement.Relation(1, map(kv), members);
    }

    private static OsmElement.Relation.Member wayMember() {
        return new OsmElement.Relation.Member(OsmElement.Type.WAY, 10, "outer");
    }

    private static OsmElement.Relation.Member relationMember() {
        return new OsmElement.Relation.Member(OsmElement.Type.RELATION, 20, "subarea");
    }

    // ---- placeKeys ----

    @Test
    public void placeKeys_derivesNameAndWikidataKeys() {
        assertEquals(List.of("name=Afula", "wikidata=Q1"),
                PlaceIndex.placeKeys(tags("name", "Afula", "wikidata", "Q1")));
        assertEquals(List.of("name=Afula"), PlaceIndex.placeKeys(tags("name", "Afula")));
        assertEquals(List.of("wikidata=Q1"), PlaceIndex.placeKeys(tags("wikidata", "Q1")));
        assertEquals(List.of(), PlaceIndex.placeKeys(tags("place", "city")));
    }

    // ---- trimPlaceTags ----

    @Test
    public void trimPlaceTags_keepsMergeableTagsAndDropsTheRest() {
        var trimmed = PlaceIndex.trimPlaceTags(map(
                "name", "Afula", "name:en", "Afula", "description", "d", "alt_name", "Affula",
                "loc_name:he", "עפולה", "wikidata", "Q1", "population", "5000",
                "boundary", "administrative", "admin_level", "8", "type", "boundary", "highway", "primary"));

        assertEquals(map("name", "Afula", "name:en", "Afula", "description", "d", "alt_name", "Affula",
                "loc_name:he", "עפולה", "wikidata", "Q1", "population", "5000"), trimmed);
    }

    // ---- estimatePopulation ----

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

    // ---- shouldIndex: ranking relation > node > way ----

    @Test
    public void shouldIndex_soleRepresentationAlwaysWins() {
        var index = new PlaceIndex();
        for (Kind kind : Kind.values()) {
            assertTrue(index.shouldIndex(kind, tags("name", "Lonely", "place", "village")),
                    kind + " alone should be indexed");
        }
    }

    @Test
    public void shouldIndex_nodeOutranksWayButNotRelation() {
        var index = new PlaceIndex();
        index.recordNode(node("place", "city", "name", "Afula"));

        var afula = tags("name", "Afula");
        assertTrue(index.shouldIndex(Kind.NODE, afula), "the node wins when there is no relation");
        assertFalse(index.shouldIndex(Kind.WAY, afula), "a way defers to the node of the same place");
        assertTrue(index.shouldIndex(Kind.WAY, tags("name", "Elsewhere")), "a different place is untouched");
    }

    @Test
    public void shouldIndex_relationOutranksNodeAndWay() {
        var index = new PlaceIndex();
        index.recordNode(node("place", "city", "name", "Nazareth"));
        index.recordRelation(relation(List.of(wayMember()), "place", "city", "name", "Nazareth", "type", "boundary"));

        assertFalse(index.shouldIndex(Kind.NODE, tags("name", "Nazareth")), "the node defers to the relation");
        assertFalse(index.shouldIndex(Kind.WAY, tags("name", "Nazareth")), "the way defers to the relation");
        assertTrue(index.shouldIndex(Kind.RELATION, tags("name", "Nazareth", "type", "boundary")));
    }

    @Test
    public void shouldIndex_relationThatWontBecomeAPolygonIsIgnored() {
        // A relation only outranks the node when planetiler will actually turn it into a
        // polygon: a polygonal type with at least one way member. Anything else is ignored,
        // so the node keeps representing the place instead of vanishing from search.
        var noWayMember = relation(List.of(relationMember()), "place", "city", "name", "Kept", "type", "boundary");
        var nonPolygonalType = relation(List.of(wayMember()), "place", "city", "name", "Kept", "type", "route");
        var noType = relation(List.of(wayMember()), "place", "city", "name", "Kept");

        for (var ignored : List.of(noWayMember, nonPolygonalType, noType)) {
            var index = new PlaceIndex();
            index.recordNode(node("place", "city", "name", "Kept"));
            index.recordRelation(ignored);
            assertTrue(index.shouldIndex(Kind.NODE, tags("name", "Kept")),
                    "the node must survive when the relation never materializes");
        }
    }

    // ---- tagsToIndex: node/way as-is, relation merges the node's tags ----

    @Test
    public void tagsToIndex_returnsNodeAndWayFeaturesUnchanged() {
        var index = new PlaceIndex();
        var feature = tags("name", "Afula", "place", "city");
        assertSame(feature, index.tagsToIndex(Kind.NODE, feature));
        assertSame(feature, index.tagsToIndex(Kind.WAY, feature));
    }

    @Test
    public void tagsToIndex_relationWithoutNodeIsUsedAsIs() {
        var index = new PlaceIndex();
        index.recordRelation(relation(List.of(wayMember()), "place", "town", "name", "NoNode", "type", "boundary"));

        var relationFeature = tags("name", "NoNode", "type", "boundary");
        assertSame(relationFeature, index.tagsToIndex(Kind.RELATION, relationFeature), "no node means nothing to merge");
    }

    @Test
    public void tagsToIndex_relationInheritsNodeTags() {
        var index = new PlaceIndex();
        index.recordNode(node("place", "city", "name", "Nazareth", "wikidata", "Q1", "population", "5000"));
        index.recordRelation(relation(List.of(wayMember()), "place", "city", "name", "Nazareth", "type", "boundary"));

        var merged = index.tagsToIndex(Kind.RELATION,
                tags("name", "Nazareth", "type", "boundary", "boundary", "administrative"));
        assertEquals("5000", merged.getString("population"), "the node's population is merged in");
        assertEquals("Q1", merged.getString("wikidata"), "the node's wikidata is merged in");
        assertEquals("administrative", merged.getString("boundary"), "the relation keeps its own tags");
    }

    @Test
    public void tagsToIndex_relationWinsOnConflictButNodeFillsGaps() {
        var index = new PlaceIndex();
        index.recordNode(node("place", "city", "name", "Old", "name:en", "OldEn", "wikidata", "Q1"));
        // The relation shares the node's wikidata but carries a different name.
        index.recordRelation(
                relation(List.of(wayMember()), "place", "city", "name", "New", "wikidata", "Q1", "type", "boundary"));

        assertFalse(index.shouldIndex(Kind.NODE, tags("name", "Old", "wikidata", "Q1")),
                "the node defers to the relation matched by shared wikidata, not name");

        var merged = index.tagsToIndex(Kind.RELATION, tags("name", "New", "wikidata", "Q1", "type", "boundary"));
        assertEquals("New", merged.getString("name"), "the relation's own name wins on conflict");
        assertEquals("OldEn", merged.getString("name:en"), "the node fills in a name the relation lacked");
    }
}
