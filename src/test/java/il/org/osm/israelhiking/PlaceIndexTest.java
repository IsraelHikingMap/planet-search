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

    private record Poly(String label, OsmElement element) {
    }

    private static Map<String, Object> tagMap(String... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static OsmElement.Relation.Member member(long ref, String role) {
        return new OsmElement.Relation.Member(OsmElement.Type.NODE, ref, role);
    }

    private static Poly way(String label, long id, String... tags) {
        return new Poly(label, new OsmElement.Way(id, tagMap(tags), new LongArrayList()));
    }

    private static Poly relation(String label, long id, List<OsmElement.Relation.Member> members, String... tags) {
        return new Poly(label, new OsmElement.Relation(id, tagMap(tags), members));
    }

    /** The labels of the polygons that survive tag/type/id dedup, in input order. */
    private static List<String> survivors(Poly... polys) {
        var index = new PlaceIndex();
        for (Poly poly : polys) {
            switch (poly.element()) {
                case OsmElement.Way w -> index.recordWayIfNeeded(w);
                case OsmElement.Relation r -> index.recordRelationIfNeeded(r);
                default -> {
                }
            }
        }
        var kept = new ArrayList<String>();
        for (Poly poly : polys) {
            if (index.isWinner(PlaceIndex.rankOf(poly.element()), poly.element().id(),
                    PlaceIndex.placeKeys(poly.element()))) {
                kept.add(poly.label());
            }
        }
        return kept;
    }

    @Test
    public void jerusalem_relationWithNodeWinsOverTheResidentialRelation() {
        var withNode = relation("relation-with-node", 1381350, List.of(member(30960212, "admin_centre")),
                "place", "city", "name", "ירושלים", "wikidata", "Q1218");
        var residential = relation("residential-relation", 6502363, List.of(),
                "place", "city", "landuse", "residential", "name", "ירושלים", "wikidata", "Q1218");
        assertEquals(List.of("relation-with-node"), survivors(withNode, residential));
    }

    @Test
    public void nesTziona_plainWayYieldsToTheResidentialWay() {
        var residential = way("residential-way", 82991026,
                "place", "town", "landuse", "residential", "name", "נס ציונה", "wikidata", "Q168162");
        var plain = way("plain-way", 38283881, "place", "town", "name", "נס ציונה");
        assertEquals(List.of("residential-way"), survivors(residential, plain));
    }

    @Test
    public void residentialWayYieldsToTheResidentialRelation() {
        var relation = relation("relation", 100, List.of(),
                "place", "town", "landuse", "residential", "name", "פלוני", "wikidata", "Q100");
        var way = way("way", 200, "place", "town", "landuse", "residential", "name", "פלוני", "wikidata", "Q100");
        assertEquals(List.of("relation"), survivors(relation, way));
    }

    @Test
    public void sameRankPolygonsAreBrokenByTheLowerId() {
        var high = way("high-id", 200, "place", "town", "name", "פלוני", "wikidata", "Q100");
        var low = way("low-id", 100, "place", "town", "name", "פלוני", "wikidata", "Q100");
        assertEquals(List.of("low-id"), survivors(high, low));
    }

    @Test
    public void polygonsWithNoSharedNameOrWikidataBothSurvive() {
        var here = way("here", 1, "place", "town", "name", "עין הראשונה", "wikidata", "Q1");
        var far = way("far", 2, "place", "town", "name", "עין השנייה", "wikidata", "Q2");
        assertEquals(List.of("here", "far"), survivors(here, far));
    }

    @Test
    public void onlyAdminCentreOrLabelNodeMakesARelationRankAsRelationWithNode() {
        // Despite the higher id, the admin_centre relation outranks the one whose only node member is a plain member.
        var withAnchor = relation("with-anchor", 2, List.of(member(50, "admin_centre")),
                "place", "city", "name", "X", "wikidata", "Q1");
        var withoutAnchor = relation("without-anchor", 1, List.of(member(51, "")),
                "place", "city", "name", "X", "wikidata", "Q1");
        assertEquals(List.of("with-anchor"), survivors(withAnchor, withoutAnchor));
    }
}
