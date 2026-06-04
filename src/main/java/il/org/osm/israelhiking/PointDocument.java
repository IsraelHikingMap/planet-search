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
  /** Length is in meters */
  public double poiLength = 0;
  public String website;
  public double[] location;
  public Float poiProminence;
  public Float poiAreaNormalized;
  public Boolean intermittent;
  public Integer population;
  public String poiFeatureClass;

  // Ranking signals. @JsonInclude(NON_NULL) omits a null one from JSON, so ES applies missing:1.0.
  public Float prominence;
  // Raw components, stored index:false for re-tuning weights without a reindex.
  public Float prom_base;
  public Float prom_qrank_norm;
  public Float prom_meta;
  public Float ele_norm;
  public Long qrank_raw;
}