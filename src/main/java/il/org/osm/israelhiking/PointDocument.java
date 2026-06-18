package il.org.osm.israelhiking;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
class PointDocument {
  public Map<String, String> name = new HashMap<String, String>();
  public Map<String, List<String>> alt_names;
  public Map<String, String> description = new HashMap<String, String>();
  public String wikidata;
  public String image;
  public String wikimedia_commons;
  public String poiCategory;
  public String poiIcon;
  public String poiIconColor;
  public String poiSource;
  public String poiDifficulty;
  public double poiLength = 0; // meters
  public String website;
  public double[] location;
  public Float poiProminence;
  public Float poiAreaNorm;
  public Boolean intermittent;
  public Integer population;
  // Distinct from poiCategory (an app/source bucket); this is the OSM feature type ("peak"/"lake"/...).
  public String poiFeatureClass;
}
