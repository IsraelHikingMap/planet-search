package il.org.osm.israelhiking;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
class PointDocument {
  public Map<String, String> name = new HashMap<String, String>();
  // Kept separate from name and searched at a lower boost; folding variants into name hurts ranking.
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

  // Computed ranking signals (null => omitted from JSON, ES uses missing:1.0). The poi* prefix marks
  // them as calculated, not raw OSM tags.
  public Float poiProminence;
  // A size proxy (a large lake outranks a same-named pond); set only for polygons.
  public Float poiAreaNorm;
  public Boolean intermittent;
  public Integer population;
  // Distinct from poiCategory (an app/source bucket); this is the OSM feature type ("peak"/"lake"/...).
  public String poiFeatureClass;
}