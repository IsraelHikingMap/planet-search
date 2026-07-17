package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    // ---- resolvesToPolygon ----

    @Test
    public void resolvesToPolygon_trueForPolygonalTypesWithAWayMember() {
        for (String type : List.of("multipolygon", "boundary", "land_area")) {
            assertTrue(PlaceIndex.resolvesToPolygon(relation(List.of(wayMember()), "type", type)),
                    type + " with a way member should resolve to a polygon");
        }
    }

    @Test
    public void resolvesToPolygon_falseWithoutAWayMember() {
        assertFalse(PlaceIndex.resolvesToPolygon(relation(List.of(relationMember()), "type", "boundary")),
                "a boundary with no way member never materializes, so it must not suppress the node");
    }

    @Test
    public void resolvesToPolygon_falseForNonPolygonalTypes() {
        assertFalse(PlaceIndex.resolvesToPolygon(relation(List.of(wayMember()), "type", "route")));
        assertFalse(PlaceIndex.resolvesToPolygon(relation(List.of(wayMember()))));
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

    // ---- hasSearchableName ----

    @Test
    public void hasSearchableName_matchesDefaultOrSupportedLanguage() {
        assertTrue(PlaceIndex.hasSearchableName(tags("name", "X"), new String[] {}));
        assertTrue(PlaceIndex.hasSearchableName(tags("name:en", "X"), new String[] { "en" }));
        assertFalse(PlaceIndex.hasSearchableName(tags("name:en", "X"), new String[] { "he" }));
        assertFalse(PlaceIndex.hasSearchableName(tags("place", "city"), new String[] { "en" }));
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

    // ---- winningTags: ranking relation > node > way ----

    @Test
    public void winningTags_soleRepresentationAlwaysWins() {
        var index = new PlaceIndex();
        for (Kind kind : Kind.values()) {
            var feature = tags("name", "Lonely", "place", "village");
            assertSame(feature, index.winningTags(kind, feature), kind + " alone should be indexed");
        }
    }

    @Test
    public void winningTags_nodeOutranksWayButNotRelation() {
        var index = new PlaceIndex();
        index.recordNode(node("place", "city", "name", "Afula"));

        var afula = tags("name", "Afula");
        assertSame(afula, index.winningTags(Kind.NODE, afula), "the node wins when there is no relation");
        assertNull(index.winningTags(Kind.WAY, afula), "a way defers to the node of the same place");
        assertNotNull(index.winningTags(Kind.WAY, tags("name", "Elsewhere")), "a different place is untouched");
    }

    @Test
    public void winningTags_relationOutranksNodeAndWay() {
        var index = new PlaceIndex();
        index.recordNode(node("place", "city", "name", "Nazareth", "wikidata", "Q1", "population", "5000"));
        index.recordRelation(relation(List.of(wayMember()), "place", "city", "name", "Nazareth", "type", "boundary"));

        assertNull(index.winningTags(Kind.NODE, tags("name", "Nazareth")), "the node defers to the relation");
        assertNull(index.winningTags(Kind.WAY, tags("name", "Nazareth")), "the way defers to the relation");

        var relationFeature = tags("name", "Nazareth", "type", "boundary", "boundary", "administrative");
        var winning = index.winningTags(Kind.RELATION, relationFeature);
        assertNotNull(winning);
        assertEquals("5000", winning.getString("population"), "the node's population is merged in");
        assertEquals("Q1", winning.getString("wikidata"), "the node's wikidata is merged in");
        assertEquals("administrative", winning.getString("boundary"), "the relation keeps its own tags");
    }

    @Test
    public void winningTags_relationWithoutNodeIsIndexedAsIs() {
        var index = new PlaceIndex();
        index.recordRelation(relation(List.of(wayMember()), "place", "town", "name", "NoNode", "type", "boundary"));

        var relationFeature = tags("name", "NoNode", "type", "boundary");
        assertSame(relationFeature, index.winningTags(Kind.RELATION, relationFeature), "no node means nothing to merge");
        assertNull(index.winningTags(Kind.WAY, tags("name", "NoNode")), "a way still defers to the relation");
    }

    @Test
    public void winningTags_relationWinsOnConflictButNodeFillsGaps() {
        var index = new PlaceIndex();
        index.recordNode(node("place", "city", "name", "Old", "name:en", "OldEn", "wikidata", "Q1"));
        // The relation shares the node's wikidata but carries a different name.
        index.recordRelation(
                relation(List.of(wayMember()), "place", "city", "name", "New", "wikidata", "Q1", "type", "boundary"));

        assertNull(index.winningTags(Kind.NODE, tags("name", "Old", "wikidata", "Q1")),
                "the node defers to the relation matched by shared wikidata, not name");

        var winning = index.winningTags(Kind.RELATION, tags("name", "New", "wikidata", "Q1", "type", "boundary"));
        assertEquals("New", winning.getString("name"), "the relation's own name wins on conflict");
        assertEquals("OldEn", winning.getString("name:en"), "the node fills in a name the relation lacked");
    }

    @Test
    public void winningTags_nonMaterializingRelationDoesNotSuppressNode() {
        var index = new PlaceIndex();
        index.recordNode(node("place", "city", "name", "Kept"));
        // A boundary relation with no way member never becomes a polygon, so it must be ignored.
        index.recordRelation(relation(List.of(relationMember()), "place", "city", "name", "Kept", "type", "boundary"));

        var kept = tags("name", "Kept");
        assertSame(kept, index.winningTags(Kind.NODE, kept), "the node must survive when the relation never materializes");
    }
}
