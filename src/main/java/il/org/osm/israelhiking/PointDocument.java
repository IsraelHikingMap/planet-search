package il.org.osm.israelhiking;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
class PointDocument {
  public Map<String, String> name = new HashMap<String, String>();
  // Variant names (alt_name/official_name/short_name/...) per language, kept separate from name
  // and searched at lower boost. Null until a variant tag is present.
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

  // Computed ranking signals (null => omitted from JSON, ES uses missing:1.0). Prefixed poi* like
  // the other computed fields so it's clear they are calculated, not raw OSM tags.
  // Composite prominence in [0,1], floored >0 so a query-time multiply never zeroes.
  public Float poiProminence;
  // Log-normalized polygon area in [0,1]; a size proxy (a large lake outranks a same-named pond).
  // Set only for polygons, null otherwise.
  public Float poiAreaNorm;
  // True for OSM intermittent=yes (seasonal water); a query-time down-weight. Null when absent.
  public Boolean intermittent;
  // Population — place/admin layer only; null for POIs.
  public Integer population;
  // Coarse feature type from the primary OSM type tags ("peak", "lake", "city", ...), used as a
  // query-time ranking signal. Distinct from poiCategory (an app/source bucket). Null when unknown.
  public String poiFeatureClass;
}