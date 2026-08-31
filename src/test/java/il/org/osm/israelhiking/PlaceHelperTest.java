package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;

import il.org.osm.israelhiking.PlaceHelper.PlaceRank;

@Tag("unit")
public class PlaceHelperTest {

    private static final GeometryFactory GF = new GeometryFactory();
    private static final String[] LANGUAGES = { "en", "he" };

    private static Map<String, Object> tagMap(String... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static WithTags tags(String... kv) {
        return WithTags.from(tagMap(kv));
    }

    private static SourceFeature node(String... kv) {
        return SimpleFeature.create(GF.createPoint(new Coordinate(35, 32)), tagMap(kv));
    }

    private static final long LABEL_NODE = 14105355380L;

    /** An admin boundary relation carrying one node member. */
    private static OsmElement.Relation boundary(String role, String... kv) {
        return new OsmElement.Relation(1386836L, tagMap(kv),
                List.of(new OsmElement.Relation.Member(OsmElement.Type.NODE, LABEL_NODE, role)));
    }

    /** The helper with the label node already seen, as the second pass leaves it. */
    private static PlaceHelper helperThatSaw(OsmElement.Relation relation, String nodePlaceKind) {
        var helper = new PlaceHelper();
        helper.recordRelationIfNeeded(relation);
        helper.captureMemberNode(LABEL_NODE, 0.5, 0.5, nodePlaceKind);
        return helper;
    }

    private static SourceFeature polygon(String... kv) {
        var square = GF.createPolygon(new Coordinate[] {
                new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1), new Coordinate(0, 1),
                new Coordinate(0, 0) });
        return SimpleFeature.create((Geometry) square, tagMap(kv));
    }

    @Test
    public void estimatePopulation_emptyForNonPlaces() {
        assertTrue(PlaceHelper.estimatePopulation(tags("natural", "peak"), null).isEmpty());
        assertTrue(PlaceHelper.estimatePopulation(tags("place", ""), "").isEmpty());
    }

    @Test
    public void estimatePopulation_readsAKindTheFeatureDoesNotCarry() {
        var boundary = tags("boundary", "administrative", "population", "81631");

        assertEquals(81631, PlaceHelper.estimatePopulation(boundary, "town").getAsInt());
        assertEquals(50_000, PlaceHelper.estimatePopulation(tags("boundary", "administrative"), "town").getAsInt());
    }

    @Test
    public void estimatePopulation_prefersTheTaggedValue() {
        assertEquals(83000, PlaceHelper.estimatePopulation(tags("place", "city", "population", "83000"), "city").getAsInt());
    }

    @Test
    public void estimatePopulation_fallsBackToDefaultsByRank() {
        assertEquals(1_000_000, PlaceHelper.estimatePopulation(tags("place", "city"), "city").getAsInt());
        assertEquals(50_000, PlaceHelper.estimatePopulation(tags("place", "town"), "town").getAsInt());
        assertEquals(2_000, PlaceHelper.estimatePopulation(tags("place", "village"), "village").getAsInt());
        assertEquals(200, PlaceHelper.estimatePopulation(tags("place", "hamlet"), "hamlet").getAsInt());
        assertEquals(20, PlaceHelper.estimatePopulation(tags("place", "isolated_dwelling"), "isolated_dwelling").getAsInt());
    }

    @Test
    public void calculatePlaceRank_isNoneForANonPlace() {
        assertEquals(PlaceRank.NONE, new PlaceHelper().calculatePlaceRank(node("natural", "peak", "name", "X")));
        assertEquals(PlaceRank.NONE, new PlaceHelper().calculatePlaceRank(polygon("landuse", "forest", "name", "X")));
    }

    @Test
    public void calculatePlaceRank_isNodeForAPlaceNode() {
        assertEquals(PlaceRank.NODE, new PlaceHelper().calculatePlaceRank(node("place", "city", "name", "X")));
    }

    @Test
    public void calculatePlaceRank_readsThePolygonKind() {
        assertEquals(PlaceRank.PLAIN, new PlaceHelper().calculatePlaceRank(polygon("place", "town", "name", "X")));
        assertEquals(PlaceRank.RESIDENTIAL_WAY,
                new PlaceHelper().calculatePlaceRank(polygon("place", "town", "landuse", "residential", "name", "X")));
    }

    // The mapping the OSM wiki asks for: the boundary is the administrative
    // area, and a label node member says which settlement it bounds.
    @Test
    public void getLabelNodePlaceKind_readsTheKindOffTheLabelNode() {
        var relation = boundary("label", "boundary", "administrative", "admin_level", "8", "name", "נצרת");

        assertEquals("town", helperThatSaw(relation, "town").getLabelNodePlaceKind(relation));
    }

    /** A boundary whose label node fell outside the extract borrows nothing. */
    @Test
    public void getLabelNodePlaceKind_isNullWhenTheLabelNodeWasNeverSeen() {
        var relation = boundary("label", "boundary", "administrative", "admin_level", "8", "name", "נצרת");
        var helper = new PlaceHelper();
        helper.recordRelationIfNeeded(relation);

        assertNull(helper.getLabelNodePlaceKind(relation));
    }

    // admin_centre is the seat of government: a regional council's is one of the
    // villages inside it, a different place from the council itself.
    @Test
    public void getLabelNodePlaceKind_doesNotReadAnAdminCentre() {
        var relation = boundary("admin_centre", "boundary", "administrative", "admin_level", "8", "name",
                "מ.א. עמק יזרעאל");

        assertNull(helperThatSaw(relation, "village").getLabelNodePlaceKind(relation));
    }

    /** Below the municipality a boundary is a quarter of a place, not the place. */
    @Test
    public void getLabelNodePlaceKind_ignoresBoundariesFinerThanAMunicipality() {
        var relation = boundary("label", "boundary", "administrative", "admin_level", "10", "name", "שכונה");

        assertNull(helperThatSaw(relation, "suburb").getLabelNodePlaceKind(relation));
    }

    @Test
    public void getPlaceNames_collectsDefaultAndLanguageNames() {
        var feature = node("name", "נצרת", "name:en", "Nazareth", "name:fr", "Nazareth-le-Fr");
        assertEquals(Set.of("נצרת", "Nazareth"), PlaceHelper.getPlaceNames(feature, LANGUAGES));
    }
}
