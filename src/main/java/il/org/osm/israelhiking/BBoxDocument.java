package il.org.osm.israelhiking;

import java.util.HashMap;
import java.util.Map;

import org.locationtech.jts.geom.Envelope;

public class BBoxDocument {
    public Map<String, String> name = new HashMap<String,String>();
    public Map<String, Object> bbox;
    public double area;

    public void setBBox(Envelope envelope) {
        bbox = new HashMap<String, Object>();
        bbox.put("type", "envelope");
        double[][] coordinates = new double[][]{{envelope.getMinX(), envelope.getMaxY()}, {envelope.getMaxX(), envelope.getMinY()}};
        
        bbox.put("coordinates", coordinates);
    }
}
