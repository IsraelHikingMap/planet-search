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
import il.org.osm.israelhiking.PlaceHelper.PlaceRank;

@Tag("unit")
public class ContainerIndexTest {

    private static final GeometryFactory GF = new GeometryFactory();

    private static Geometry square(double minLon, double minLat, double maxLon, double maxLat) {
        return GF.createPolygon(new Coordinate[] {
                new Coordinate(minLon, minLat), new Coordinate(maxLon, minLat),
                new Coordinate(maxLon, maxLat), new Coordinate(minLon, maxLat),
                new Coordinate(minLon, minLat) });
    }

    private static double[] centerOf(Geometry geom) {
        var c = geom.getCentroid().getCoordinate();
        return new double[] { c.x, c.y };
    }

    private static ContainerRecord place(PlaceRank rank, long id, String wikidata, Map<String, String> names,
            Geometry geom) {
        double[] c = centerOf(geom);
        return new ContainerRecord(names, 0, geom.getArea(), geom, wikidata, rank, id, c[0], c[1]);
    }

    private static ContainerRecord nonPlace(long id, Map<String, String> names, Geometry geom) {
        double[] c = centerOf(geom);
        return new ContainerRecord(names, 0, geom.getArea(), geom, null, PlaceRank.NONE, id, c[0], c[1]);
    }

    private static ContainerIndex indexOf(ContainerRecord... records) {
        return new ContainerIndex(List.of(records));
    }

    @Test
    public void nodeYieldsToAnEnclosingPlacePolygonSharingTheName() {
        var index = indexOf(place(PlaceRank.PLAIN, 1, "Q111", Map.of("he", "נצרת"), square(0, 0, 0.1, 0.1)));
        assertTrue(index.coveredByBetterPlace(0.05, 0.05, Set.of("נצרת"), "Q430", PlaceRank.NODE, 278477461),
                "the node sits inside a place polygon of the same name (different wikidata)");
    }

    @Test
    public void nodeYieldsToAnEnclosingPlacePolygonSharingTheWikidata() {
        var index = indexOf(place(PlaceRank.PLAIN, 1, "Q1", Map.of("he", "שם אחר"), square(0, 0, 0.1, 0.1)));
        assertTrue(index.coveredByBetterPlace(0.05, 0.05, Set.of("X"), "Q1", PlaceRank.NODE, 2),
                "matched by wikidata even though the names differ");
    }

    @Test
    public void featureIsNotYieldedToANonPlacePolygon() {
        var index = indexOf(nonPlace(1, Map.of("he", "נצרת"), square(0, 0, 0.1, 0.1)));
        assertFalse(index.coveredByBetterPlace(0.05, 0.05, Set.of("נצרת"), "Q430", PlaceRank.NODE, 2),
                "a boundary that is not itself a place must not cover the feature");
    }

    @Test
    public void featureIsNotYieldedWhenFarAwayAndNotEnclosed() {
        var index = indexOf(place(PlaceRank.PLAIN, 1, "Q1", Map.of("he", "נצרת"), square(0, 0, 0.1, 0.1)));
        assertFalse(index.coveredByBetterPlace(50, 50, Set.of("נצרת"), "Q1", PlaceRank.NODE, 2),
                "the point is neither inside the polygon nor near its centre");
    }

    @Test
    public void featureIsNotYieldedWhenNeitherNameNorWikidataMatch() {
        var index = indexOf(place(PlaceRank.PLAIN, 1, "Q1", Map.of("he", "נצרת"), square(0, 0, 0.1, 0.1)));
        assertFalse(index.coveredByBetterPlace(0.05, 0.05, Set.of("תל אביב"), "Q2", PlaceRank.NODE, 2),
                "a different place that happens to sit inside is not covered");
    }

    @Test
    public void plainPolygonYieldsToANearbyResidentialPolygonItDoesNotEnclose() {
        var residential = place(PlaceRank.RESIDENTIAL_WAY, 82991026, "Q168162", Map.of("he", "נס ציונה"),
                square(34.79, 31.925, 34.81, 31.94));
        var index = indexOf(residential);
        assertFalse(residential.geometry.contains(GF.createPoint(new Coordinate(34.795, 31.9229))),
                "precondition: the plain centre is not inside the residential polygon");
        assertTrue(index.coveredByBetterPlace(31.9229, 34.795, Set.of("נס ציונה"), null, PlaceRank.PLAIN, 38283881),
                "centres ~0.9 km apart, so the weaker plain way yields");
    }

    @Test
    public void sameNamedPlaceFartherThanAFewKmIsKept() {
        var index = indexOf(place(PlaceRank.PLAIN, 1, "Q1", Map.of("he", "עין X"), square(0, 0, 0.02, 0.02)));
        assertFalse(index.coveredByBetterPlace(0.11, 0.01, Set.of("עין X"), "Q2", PlaceRank.PLAIN, 2),
                "a same-named place several km away and not enclosed is kept");
    }

    @Test
    public void polygonIsNotYieldedToAWeakerPolygon() {
        var index = indexOf(place(PlaceRank.PLAIN, 1, "Q1", Map.of("he", "X"), square(0, 0, 0.1, 0.1)));
        assertFalse(index.coveredByBetterPlace(0.05, 0.05, Set.of("X"), "Q1", PlaceRank.RESIDENTIAL_WAY, 2),
                "a plain container does not outrank a residential feature");
    }

    @Test
    public void sameRankIsBrokenByTheLowerId() {
        var index = indexOf(place(PlaceRank.PLAIN, 100, "Q1", Map.of("he", "X"), square(0, 0, 0.1, 0.1)));
        assertTrue(index.coveredByBetterPlace(0.05, 0.05, Set.of("X"), "Q1", PlaceRank.PLAIN, 200),
                "the lower-id container wins the tie");
        assertFalse(index.coveredByBetterPlace(0.05, 0.05, Set.of("X"), "Q1", PlaceRank.PLAIN, 50),
                "a higher-id container does not suppress the lower-id feature");
    }

    @Test
    public void aPolygonDoesNotYieldToItself() {
        var index = indexOf(place(PlaceRank.PLAIN, 7, "Q1", Map.of("he", "X"), square(0, 0, 0.1, 0.1)));
        assertFalse(index.coveredByBetterPlace(0.05, 0.05, Set.of("X"), "Q1", PlaceRank.PLAIN, 7),
                "the same element (same rank, same id) is not better than itself");
    }
}
