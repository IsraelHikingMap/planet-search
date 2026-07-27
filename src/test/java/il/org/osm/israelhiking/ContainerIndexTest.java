package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import il.org.osm.israelhiking.ContainerIndex.ContainerRecord;

@Tag("unit")
public class ContainerIndexTest {

    private static final GeometryFactory GF = new GeometryFactory();

    private static Geometry square(double minLon, double minLat, double maxLon, double maxLat) {
        return GF.createPolygon(new Coordinate[] {
                new Coordinate(minLon, minLat), new Coordinate(maxLon, minLat),
                new Coordinate(maxLon, maxLat), new Coordinate(minLon, maxLat),
                new Coordinate(minLon, minLat) });
    }

    private static ContainerRecord place(String wikidata, Map<String, String> names, Geometry geom) {
        return new ContainerRecord(names, 0, geom.getArea(), geom, true, wikidata);
    }

    private static ContainerRecord nonPlace(String wikidata, Map<String, String> names, Geometry geom) {
        return new ContainerRecord(names, 0, geom.getArea(), geom, false, wikidata);
    }

    private static ContainerIndex indexOf(ContainerRecord... records) {
        return new ContainerIndex(List.of(records));
    }

    @Test
    public void nodeYieldsToAnEnclosingPlacePolygonSharingTheName() {
        var index = indexOf(place("Q111", Map.of("he", "נצרת"), square(0, 0, 10, 10)));
        assertTrue(index.enclosesSamePlace(5, 5, Set.of("נצרת"), "Q430"),
                "the node sits inside a place polygon of the same name (different wikidata)");
    }

    @Test
    public void nodeYieldsToAnEnclosingPlacePolygonSharingTheWikidata() {
        var index = indexOf(place("Q1", Map.of("he", "שם אחר"), square(0, 0, 10, 10)));
        assertTrue(index.enclosesSamePlace(5, 5, Set.of("X"), "Q1"),
                "matched by wikidata even though the names differ");
    }

    @Test
    public void nodeIsNotYieldedToANonPlacePolygon() {
        var index = indexOf(nonPlace("Q430", Map.of("he", "נצרת"), square(0, 0, 10, 10)));
        assertFalse(index.enclosesSamePlace(5, 5, Set.of("נצרת"), "Q430"),
                "a boundary that is not itself a place must not cover the node");
    }

    @Test
    public void nodeIsNotYieldedWhenOutsideThePolygon() {
        var index = indexOf(place("Q1", Map.of("he", "נצרת"), square(0, 0, 10, 10)));
        assertFalse(index.enclosesSamePlace(50, 50, Set.of("נצרת"), "Q1"), "the node is outside");
    }

    @Test
    public void nodeIsNotYieldedWhenNeitherNameNorWikidataMatch() {
        var index = indexOf(place("Q1", Map.of("he", "נצרת"), square(0, 0, 10, 10)));
        assertFalse(index.enclosesSamePlace(5, 5, Set.of("תל אביב"), "Q2"),
                "a different place that happens to sit inside is not covered");
    }
}
