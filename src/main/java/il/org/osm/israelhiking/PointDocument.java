package il.org.osm.israelhiking;

import java.util.HashMap;
import java.util.Map;

class PointDocument {
    public Map<String, String> name = new HashMap<String,String>();
    public Map<String, String> description = new HashMap<String,String>();
    public String wikidata;
    public String image;
    public String wikimedia_commons;
    public String poiCategory;
    public String poiIcon;
    public String poiIconColor;
    public double[] location;
  }