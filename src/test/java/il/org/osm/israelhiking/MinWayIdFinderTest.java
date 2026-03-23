package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;

@Tag("unit")
public class MinWayIdFinderTest {

    private SourceFeature makeLineFeature(long id, double x1, double y1, double x2, double y2) {
        Geometry line = new GeometryFactory().createLineString(new Coordinate[] {
                new Coordinate(x1, y1),
                new Coordinate(x2, y2)
        });
        return SimpleFeature.create(line, Map.of(), "OSM", "Lines", id);
    }

    @Test
    public void shouldFindMinWayWhenAddingBackwards() throws GeometryException {
        var finder = new MinWayIdFinder();
        finder.features.add(makeLineFeature(7L, 0, 0, 1, 1));
        finder.features.add(makeLineFeature(3L, 1, 1, 2, 2));
        var merged = finder.getMergedFeatures();
        assertEquals(1, merged.size());
        assertEquals(3L, merged.getFirst().minId);
    }

    @Test
    public void shouldFindMinWayWhenAddingForward() throws GeometryException {
        var finder = new MinWayIdFinder();
        finder.features.add(makeLineFeature(3L, 1, 1, 2, 2));
        finder.features.add(makeLineFeature(7L, 0, 0, 1, 1)); // connects to above
        var merged = finder.getMergedFeatures();
        assertEquals(1, merged.size());
        assertEquals(3L, merged.getFirst().minId);
    }

    @Test
    public void shouldNotMergeDisconnectedFeatures() throws GeometryException {
        var finder = new MinWayIdFinder();
        finder.features.add(makeLineFeature(3L, 0, 0, 1, 1));
        finder.features.add(makeLineFeature(7L, 2, 2, 3, 3));
        var merged = finder.getMergedFeatures();
        assertEquals(2, merged.size());
    }

    @Test
    public void shouldMergeMultipleConnectedFeatures() throws GeometryException {
        var finder = new MinWayIdFinder();
        finder.features.add(makeLineFeature(5L, 0, 0, 1, 1));
        finder.features.add(makeLineFeature(2L, 2, 2, 3, 3));
        finder.features.add(makeLineFeature(9L, 1, 1, 2, 2));
        var merged = finder.getMergedFeatures();
        assertEquals(1, merged.size());
        assertEquals(2L, merged.getFirst().minId);
    }

    @Test
    public void shouldMergeSelfClosingFeatures() throws GeometryException {
        var finder = new MinWayIdFinder();
        finder.features.add(makeLineFeature(5L, 0, 0, 1, 1));
        finder.features.add(makeLineFeature(2L, 2, 2, 3, 3));
        finder.features.add(makeLineFeature(9L, 1, 1, 2, 2));
        finder.features.add(makeLineFeature(10L, 3, 3, 0, 0));
        var merged = finder.getMergedFeatures();
        assertEquals(1, merged.size());
        assertEquals(2L, merged.getFirst().minId);
    }
}