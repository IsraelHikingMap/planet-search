package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
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

    private static SourceFeature polygon(String... kv) {
        var square = GF.createPolygon(new Coordinate[] {
                new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1), new Coordinate(0, 1),
                new Coordinate(0, 0) });
        return SimpleFeature.create((Geometry) square, tagMap(kv));
    }

    @Test
    public void estimatePopulation_emptyForNonPlaces() {
        assertTrue(PlaceHelper.estimatePopulation(tags("natural", "peak")).isEmpty());
        assertTrue(PlaceHelper.estimatePopulation(tags("place", "")).isEmpty());
    }

    @Test
    public void estimatePopulation_prefersTheTaggedValue() {
        assertEquals(83000, PlaceHelper.estimatePopulation(tags("place", "city", "population", "83000")).getAsInt());
    }

    @Test
    public void estimatePopulation_fallsBackToDefaultsByRank() {
        assertEquals(1_000_000, PlaceHelper.estimatePopulation(tags("place", "city")).getAsInt());
        assertEquals(50_000, PlaceHelper.estimatePopulation(tags("place", "town")).getAsInt());
        assertEquals(2_000, PlaceHelper.estimatePopulation(tags("place", "village")).getAsInt());
        assertEquals(200, PlaceHelper.estimatePopulation(tags("place", "hamlet")).getAsInt());
        assertEquals(20, PlaceHelper.estimatePopulation(tags("place", "isolated_dwelling")).getAsInt());
    }

    @Test
    public void calculatePlaceRank_isNoneForANonPlace() {
        assertEquals(PlaceRank.NONE, PlaceHelper.calculatePlaceRank(node("natural", "peak", "name", "X")));
        assertEquals(PlaceRank.NONE, PlaceHelper.calculatePlaceRank(polygon("landuse", "forest", "name", "X")));
    }

    @Test
    public void calculatePlaceRank_isNodeForAPlaceNode() {
        assertEquals(PlaceRank.NODE, PlaceHelper.calculatePlaceRank(node("place", "city", "name", "X")));
    }

    @Test
    public void calculatePlaceRank_readsThePolygonKind() {
        assertEquals(PlaceRank.PLAIN, PlaceHelper.calculatePlaceRank(polygon("place", "town", "name", "X")));
        assertEquals(PlaceRank.RESIDENTIAL_WAY,
                PlaceHelper.calculatePlaceRank(polygon("place", "town", "landuse", "residential", "name", "X")));
    }

    @Test
    public void getPlaceNames_collectsDefaultAndLanguageNames() {
        var feature = node("name", "נצרת", "name:en", "Nazareth", "name:fr", "Nazareth-le-Fr");
        assertEquals(Set.of("נצרת", "Nazareth"), PlaceHelper.getPlaceNames(feature, LANGUAGES));
    }
}
