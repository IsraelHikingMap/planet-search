package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;


@Tag("unit")
public class LineMergerTest {
    
    @Test
    public void simpleLine_ShouldReturnFirstPoint() {
        var merger = new LineMerger();
        var factory = new GeometryFactory();
        merger.add(factory.createLineString(new Coordinate[] {new Coordinate(0, 0), new Coordinate(1, 1)}));
        assertTrue(merger.getFirstPoint().equals2D(new Coordinate(0, 0)));
    }

    @Test
    public void lineWithLoop_ShouldReturnFirstPoint() {
        var merger = new LineMerger();
        var factory = new GeometryFactory();
        merger.add(factory.createLineString(new Coordinate[] {new Coordinate(0, 0), new Coordinate(1, 1)}));
        merger.add(factory.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(2, 2), new Coordinate(1, 1)}));
        assertTrue(merger.getFirstPoint().equals2D(new Coordinate(0, 0)));
    }
}
