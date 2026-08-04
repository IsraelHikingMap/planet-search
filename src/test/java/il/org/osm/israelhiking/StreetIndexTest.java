package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import com.onthegomap.planetiler.reader.SimpleFeature;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;

@Tag("unit")
public class StreetIndexTest {

    private SimpleFeature street(long id, Map<String, Object> tags) {
        Geometry line = new GeometryFactory().createLineString(new Coordinate[] {
                new Coordinate(0, 0), new Coordinate(1, 1) });
        return SimpleFeature.create(line, tags, "OSM", "Lines", id);
    }

    // A street document as a worker thread hands it over: a name and a point,
    // with no container names on it yet — those are added by the enricher the
    // flush runs, which here just tags the document with the city it is in.
    private PointDocument document(String name, double lat, double lng) {
        var pointDocument = new PointDocument();
        pointDocument.name = Map.of("default", name);
        pointDocument.location = new double[] { lng, lat };
        return pointDocument;
    }

    // Stands in for the container index: the container a segment was keyed by
    // only gets its name at flush time, as a real build does.
    private List<String> indexedIds(StreetIndex helper, Function<PointDocument, String> cityOf) {
        var operations = new ArrayList<BulkOperation>();
        helper.flush(operations::add, "points", pointDocument -> {
            var city = cityOf.apply(pointDocument);
            if (city != null) {
                pointDocument.poiContainer = Map.of("default", city);
            }
        });
        return operations.stream().map(op -> op.index().id()).toList();
    }

    /** No point in this test falls in a container. */
    private static final Function<PointDocument, String> NO_CITY = pointDocument -> null;

    // The container handles a build hands to add(): opaque, only their identity
    // matters, and 0 means the segment falls in no container at all.
    private static final long HAIFA = 1L;
    private static final long NETANYA = 2L;
    private static final long NO_CONTAINER = 0L;

    @Test
    public void isStreetAcceptsNamedRoutableHighwaysOnly() {
        assertTrue(StreetIndex.isStreet(street(1L, Map.of("highway", "residential", "name", "הרצל"))));
        assertFalse(StreetIndex.isStreet(street(2L, Map.of("highway", "residential"))));
        assertFalse(StreetIndex.isStreet(street(3L, Map.of("highway", "path", "name", "שביל"))));
        assertFalse(StreetIndex.isStreet(street(4L, Map.of("name", "הרצל"))));
    }

    @Test
    public void mergesSameNameAndCityIntoOneStreetAtMinId() {
        var helper = new StreetIndex();
        helper.add(7L, document("הרצל", 32.0, 34.0), HAIFA);
        helper.add(3L, document("הרצל", 32.0, 34.0), HAIFA);
        helper.add(9L, document("הרצל", 32.0, 34.0), HAIFA);

        assertEquals(List.of("OSM_way_3"), indexedIds(helper, pointDocument -> "חיפה"));
    }

    @Test
    public void keepsSameNameInDifferentCitiesApart() {
        var helper = new StreetIndex();
        helper.add(7L, document("הרצל", 32.0, 34.0), HAIFA);
        helper.add(5L, document("הרצל", 32.3, 34.85), NETANYA);

        assertEquals(2, indexedIds(helper,
                pointDocument -> pointDocument.location[1] > 32.1 ? "נתניה" : "חיפה").size());
    }

    @Test
    public void scopesByGridCellWhenThereIsNoCity() {
        var helper = new StreetIndex();
        helper.add(7L, document("דרך", 30.0, 34.0), NO_CONTAINER);
        helper.add(3L, document("דרך", 30.0, 34.0), NO_CONTAINER);
        helper.add(5L, document("דרך", 31.0, 35.0), NO_CONTAINER);

        assertEquals(2, indexedIds(helper, NO_CITY).size());
    }

    // A street long enough to span two grid cells reaches the flush as two
    // candidates; both resolve to the same city, so it is one street again.
    @Test
    public void mergesCandidatesThatResolveToTheSameCity() {
        var helper = new StreetIndex();
        helper.add(7L, document("הרצל", 32.0, 34.0), NO_CONTAINER);
        helper.add(3L, document("הרצל", 32.4, 34.4), NO_CONTAINER);

        assertEquals(List.of("OSM_way_3"), indexedIds(helper, pointDocument -> "חיפה"));
    }
}
