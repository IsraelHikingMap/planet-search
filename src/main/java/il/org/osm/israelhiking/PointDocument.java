package il.org.osm.israelhiking;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
class PointDocument {
  public Map<String, String> name = new HashMap<String, String>();
  /**
   * ADR-0011 item 1 — variant/alternative names (alt_name, alt_name:&lt;lang&gt;, official_name,
   * short_name, loc_name, int_name, ...) keyed by language. SEPARATE from {@link #name} on purpose
   * (folding variants into name would break ranking/display/ADR-0009). Searched at strictly lower
   * boosts than name and never read by display (HitToFeature keeps the canonical name).
   *
   * <p>Left null until at least one variant tag is present (NON_NULL on the class then omits it from
   * JSON), so the ~22.8M docs with no alt names carry no extra field.
   */
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

  // --- Ranking signals (added for search relevance; null => omitted from JSON, ES uses missing:1.0) ---
  /** Composite prominence score in [0,1], floored >0 so a query-time multiply never zeroes. Hot path. */
  public Float prominence;
  /** Raw components, stored index:false for re-tuning weights without a reindex. */
  public Float prom_base;
  public Float prom_qrank_norm;
  public Float prom_meta;
  public Float ele_norm;
  public Long qrank_raw;
  /**
   * ADR-0014 — log-normalized polygon area in [0,1] ({@code log1p(areaMeters)/log1p(MAX_AREA_M2)}),
   * a prominence proxy for outdoor features (a large lake outranks a same-named pond). Computed only
   * when {@code feature.canBePolygon()}; null for pure points/lines (omitted from JSON). index:false —
   * a query-time scoring signal, re-weighted in the script without a reindex.
   */
  public Float area_norm;
  /**
   * ADR-0014 — true when the OSM {@code intermittent=yes} tag is present (seasonal/dry water bodies
   * such as the TX "Tule Lake" reservoir). A query-time DOWN-weight for non-real water. Null when the
   * tag is absent (omitted from JSON).
   */
  public Boolean intermittent;
  /** Population — place/admin layer only (city/town/village...); null for POIs. */
  public Integer population;
  /**
   * Coarse FEATURE TYPE derived from the primary OSM type tags (natural/place/waterway/...), e.g.
   * "peak", "lake", "waterfall", "city", "spring". Distinct from {@link #poiCategory}, which is an
   * app/source bucket ("Wikipedia"/"Hiking"/"Other") and does NOT identify the kind of feature.
   * Used as a query-time ranking signal (boost docs whose class matches the query's intent) — see
   * the rescore scoring model. Null when no recognised type tag is present (omitted from JSON).
   */
  public String feature_class;
}