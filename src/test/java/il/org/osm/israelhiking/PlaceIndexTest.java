package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;

@Tag("unit")
public class PlaceIndexTest {

    private static WithTags tags(String... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return WithTags.from(m);
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

    private record Rep(String label, OsmElement element, int rank) {
    }

    private static Map<String, Object> tagMap(String... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static OsmElement.Relation.Member nodeMember(long ref) {
        return new OsmElement.Relation.Member(OsmElement.Type.NODE, ref, "admin_centre");
    }

    private static Rep node(String label, long id, String... tags) {
        return new Rep(label, new OsmElement.Node(id, tagMap(tags), 32.0, 35.0), PlaceIndex.RANK_NODE);
    }

    private static Rep way(String label, long id, int rank, String... tags) {
        return new Rep(label, new OsmElement.Way(id, tagMap(tags), new LongArrayList()), rank);
    }

    private static Rep relation(String label, long id, int rank, List<OsmElement.Relation.Member> members,
            String... tags) {
        return new Rep(label, new OsmElement.Relation(id, tagMap(tags), members), rank);
    }

    /** The labels of the representations that survive dedup, in input order. */
    private static List<String> survivors(Rep... reps) {
        var index = new PlaceIndex();
        for (Rep rep : reps) {
            switch (rep.element()) {
                case OsmElement.Node n -> index.recordNodeIfNeeded(n);
                case OsmElement.Way w -> index.recordWayIfNeeded(w);
                case OsmElement.Relation r -> index.recordRelationIfNeeded(r);
                default -> {
                }
            }
        }
        var kept = new ArrayList<String>();
        for (Rep rep : reps) {
            if (index.isWinner(rep.rank(), rep.element().id(), PlaceIndex.placeKeys(rep.element()))) {
                kept.add(rep.label());
            }
        }
        return kept;
    }

    @Test
    public void nazareth_nodeYieldsToTheTownRelation() {
        var relation = relation("relation", 17394564, PlaceIndex.RANK_PLAIN, List.of(),
                "place", "town", "name", "נצרת", "wikidata", "Q111997770");
        var node = node("node", 278477461, "place", "town", "name", "נצרת", "wikidata", "Q430776");
        assertEquals(List.of("relation"), survivors(relation, node));
    }

    @Test
    public void jerusalem_relationWithNodeWinsOverTheResidentialRelation() {
        var withNode = relation("relation-with-node", 1381350, PlaceIndex.RANK_RELATION_WITH_NODE,
                List.of(nodeMember(30960212)), "place", "city", "name", "ירושלים", "wikidata", "Q1218");
        var residential = relation("residential-relation", 6502363, PlaceIndex.RANK_RESIDENTIAL_RELATION, List.of(),
                "place", "city", "landuse", "residential", "name", "ירושלים", "wikidata", "Q1218");
        var node = node("node", 30960212, "place", "city", "name", "ירושלים", "wikidata", "Q1218");
        assertEquals(List.of("relation-with-node"), survivors(withNode, residential, node));
    }

    @Test
    public void nesTziona_plainWayYieldsToTheResidentialWay() {
        var residential = way("residential-way", 82991026, PlaceIndex.RANK_RESIDENTIAL_WAY,
                "place", "town", "landuse", "residential", "name", "נס ציונה", "wikidata", "Q168162");
        var plain = way("plain-way", 38283881, PlaceIndex.RANK_PLAIN, "place", "town", "name", "נס ציונה");
        var node = node("node", 202433223, "place", "town", "name", "נס ציונה", "wikidata", "Q168162");
        assertEquals(List.of("residential-way"), survivors(residential, plain, node));
    }

    @Test
    public void residentialWayYieldsToTheResidentialRelation() {
        var relation = relation("relation", 100, PlaceIndex.RANK_RESIDENTIAL_RELATION, List.of(),
                "place", "town", "landuse", "residential", "name", "פלוני", "wikidata", "Q100");
        var way = way("way", 200, PlaceIndex.RANK_RESIDENTIAL_WAY,
                "place", "town", "landuse", "residential", "name", "פלוני", "wikidata", "Q100");
        assertEquals(List.of("relation"), survivors(relation, way));
    }

    @Test
    public void sameRankPolygonsAreBrokenByTheLowerId() {
        var high = way("high-id", 200, PlaceIndex.RANK_PLAIN, "place", "town", "name", "פלוני", "wikidata", "Q100");
        var low = way("low-id", 100, PlaceIndex.RANK_PLAIN, "place", "town", "name", "פלוני", "wikidata", "Q100");
        assertEquals(List.of("low-id"), survivors(high, low));
    }

    @Test
    public void placesWithNoSharedNameOrWikidataBothSurvive() {
        var here = node("here", 1, "place", "village", "name", "עין הראשונה", "wikidata", "Q1");
        var far = node("far", 2, "place", "village", "name", "עין השנייה", "wikidata", "Q2");
        assertEquals(List.of("here", "far"), survivors(here, far));
    }

    @Test
    public void sameNameCollapsesEvenWithDifferentWikidata() {
        var first = node("first", 1, "place", "village", "name", "עין X", "wikidata", "Q1");
        var second = node("second", 2, "place", "village", "name", "עין X", "wikidata", "Q2");
        assertEquals(List.of("first"), survivors(first, second));
    }
}
