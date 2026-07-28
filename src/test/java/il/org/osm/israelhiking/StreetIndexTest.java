package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    // An enriched street document: a name, a point, and the city it was tagged
    // with (a null city falls back to the point's grid cell for scoping).
    private PointDocument document(String name, String city, double lat, double lng) {
        var pointDocument = new PointDocument();
        pointDocument.name = Map.of("default", name);
        pointDocument.location = new double[] { lng, lat };
        pointDocument.poiContainer = city == null ? null : Map.of("default", city);
        return pointDocument;
    }

    private List<String> indexedIds(StreetIndex helper) {
        var operations = new ArrayList<BulkOperation>();
        helper.flush(operations::add, "points");
        return operations.stream().map(op -> op.index().id()).toList();
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
        var helper = new StreetIndex();
        helper.add(7L, document("הרצל", "חיפה", 32.0, 34.0));
        helper.add(3L, document("הרצל", "חיפה", 32.0, 34.0));
        helper.add(9L, document("הרצל", "חיפה", 32.0, 34.0));

        assertEquals(List.of("OSM_way_3"), indexedIds(helper));
    }

    @Test
    public void keepsSameNameInDifferentCitiesApart() {
        var helper = new StreetIndex();
        helper.add(7L, document("הרצל", "חיפה", 32.0, 34.0));
        helper.add(5L, document("הרצל", "נתניה", 32.3, 34.85));

        assertEquals(2, indexedIds(helper).size());
    }

    @Test
    public void scopesByGridCellWhenThereIsNoCity() {
        var helper = new StreetIndex();
        helper.add(7L, document("דרך", null, 30.0, 34.0));
        helper.add(3L, document("דרך", null, 30.0, 34.0));
        helper.add(5L, document("דרך", null, 31.0, 35.0));

        assertEquals(2, indexedIds(helper).size());
    }
}
