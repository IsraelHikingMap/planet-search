package il.org.osm.israelhiking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;


public class BBoxDocument {
    public Map<String, String> name = new HashMap<String,String>();
    public Map<String, Object> bbox;
    public double area;

    public void setBBox(Geometry geometry) {
        bbox = new HashMap<String, Object>();
        bbox.put("type", geometry.getGeometryType());        
        bbox.put("coordinates", convertJTSToGeoJson(geometry));
    }

    private Object convertJTSToGeoJson(Geometry geometry) {
        if (geometry instanceof Polygon) {
            return convertPolygonToGeoJson((Polygon) geometry);
        } else if (geometry instanceof MultiPolygon) {
            return convertMultiPolygonToGeoJson((MultiPolygon) geometry);
        }
        throw new UnsupportedOperationException("Geometry type not supported: " + geometry.getGeometryType());
    }

    private List<List<List<Double>>> convertPolygonToGeoJson(Polygon polygon) {
        List<List<List<Double>>> polygonCoordinates = new ArrayList<>();
        
        // Add exterior ring
        polygonCoordinates.add(convertLinearRingToList(polygon.getExteriorRing()));
        
        // Add interior rings (holes)
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            polygonCoordinates.add(convertLinearRingToList(polygon.getInteriorRingN(i)));
        }
        
        return polygonCoordinates;
    }

    private List<List<List<List<Double>>>> convertMultiPolygonToGeoJson(MultiPolygon multiPolygon) {
        List<List<List<List<Double>>>> multiPolygonCoordinates = new ArrayList<>();
        
        for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
            Polygon polygon = (Polygon) multiPolygon.getGeometryN(i);
            multiPolygonCoordinates.add(convertPolygonToGeoJson(polygon));
        }
        
        return multiPolygonCoordinates;
    }

    private List<List<Double>> convertLinearRingToList(LineString ring) {
        List<List<Double>> coordinates = new ArrayList<>();
        Coordinate[] coords = ring.getCoordinates();
        
        for (Coordinate coord : coords) {
            coordinates.add(Arrays.asList(coord.x, coord.y));
        }
        
        return coordinates;
    }
}
