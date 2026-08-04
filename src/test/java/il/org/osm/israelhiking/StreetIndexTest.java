package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import com.onthegomap.planetiler.reader.SimpleFeature;

@Tag("unit")
public class StreetIndexTest {

    private SimpleFeature street(long id, Map<String, Object> tags) {
        Geometry line = new GeometryFactory().createLineString(new Coordinate[] {
                new Coordinate(0, 0), new Coordinate(1, 1) });
        return SimpleFeature.create(line, tags, "OSM", "Lines", id);
    }

    /** No point in this test falls in a container. */
    private static final String NO_CONTAINER = null;

    // These tests only exercise the merge, which needs none of what the flush
    // reads the OSM input with, so none of it is wired up here.
    private StreetIndex mergeOnly() {
        return new StreetIndex(operation -> {
        }, "points", Path.of("never-read.osm.pbf"), 1, null);
    }

    @Test
    public void isStreetAcceptsNamedRoutableHighwaysOnly() {
        assertTrue(StreetIndex.isStreet(street(1L, Map.of("highway", "residential", "name", "הרצל"))));
        assertFalse(StreetIndex.isStreet(street(2L, Map.of("highway", "residential"))));
        assertFalse(StreetIndex.isStreet(street(3L, Map.of("highway", "path", "name", "שביל"))));
        assertFalse(StreetIndex.isStreet(street(4L, Map.of("name", "הרצל"))));
    }

    @Test
    public void mergesSameNameAndCityIntoOneStreetAtMinId() {
        var helper = mergeOnly();
        helper.add(7L, "הרצל", "חיפה", 34.0, 32.0);
        helper.add(3L, "הרצל", "חיפה", 34.01, 32.01);
        helper.add(9L, "הרצל", "חיפה", 34.02, 32.02);

        assertArrayEquals(new long[] { 3L }, helper.winningWayIds());
    }

    @Test
    public void keepsSameNameInDifferentCitiesApart() {
        var helper = mergeOnly();
        helper.add(7L, "הרצל", "חיפה", 34.0, 32.0);
        helper.add(5L, "הרצל", "נתניה", 34.85, 32.3);

        assertArrayEquals(new long[] { 5L, 7L }, helper.winningWayIds());
    }

    // A street keeps its own identity across the whole city, however far its
    // segments are from each other — the city, not distance, is the scope.
    @Test
    public void mergesSegmentsFarApartWithinTheSameCity() {
        var helper = mergeOnly();
        helper.add(7L, "הרצל", "חיפה", 34.0, 32.0);
        helper.add(3L, "הרצל", "חיפה", 34.4, 32.4);

        assertArrayEquals(new long[] { 3L }, helper.winningWayIds());
    }

    @Test
    public void scopesByGridCellWhenThereIsNoCity() {
        var helper = mergeOnly();
        helper.add(7L, "דרך", NO_CONTAINER, 34.0, 30.0);
        helper.add(3L, "דרך", NO_CONTAINER, 34.0, 30.0);
        helper.add(5L, "דרך", NO_CONTAINER, 35.0, 31.0);

        assertArrayEquals(new long[] { 3L, 5L }, helper.winningWayIds());
    }

    @Test
    public void keepsDifferentNamesInOneCityApart() {
        var helper = mergeOnly();
        helper.add(7L, "הרצל", "חיפה", 34.0, 32.0);
        helper.add(3L, "ביאליק", "חיפה", 34.0, 32.0);

        assertEquals(2, helper.winningWayIds().length);
    }
}
