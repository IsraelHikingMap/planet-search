package il.org.osm.israelhiking;

/**
 * Computes a composite prominence score in [0,1], used at query time as a field_value_factor
 * multiplier so a major peak / well-known place outranks an obscure node with the same name.
 *
 * Formula: prominence = clamp01( 0.05 + 0.45*base + 0.40*qnorm + 0.10*meta )
 *   - floor 0.05: never zero, so a query-time multiply never annihilates a hit.
 *   - base: feature-class prior (elevation-scaled peaks, place hierarchy, parks, water, baseline).
 *   - qnorm: log-normalized QRank (Wikimedia pageviews), 0 when no wikidata/QRank hit.
 *   - meta: richness from image / website / wikidata tags.
 *
 * Pure and side-effect-free so it is unit-testable without planetiler/ES.
 */
final class ProminenceCalculator {

  /** QRank reference ceiling for log-normalization — a realistic famous-place QRank. Calibrate. */
  static final double QRANK_REF = 3_000_000.0;
  /** Highest summit on Earth (m), used to normalize elevation. */
  static final double MAX_ELE_M = 8849.0;
  /** Floor so prominence is never 0 (a 0 would zero out a multiply). */
  static final double FLOOR = 0.05;

  private ProminenceCalculator() {}

  /**
   * Holds the final prominence score plus the components that fed it. Only prominence is written to
   * the document; the components are kept so ProminenceCalculatorTest can assert on the composite
   * math (e.g. eleNorm is high for a tall peak). They are not stored in Elasticsearch.
   */
  static final class Result {
    final float prominence;
    final float base;
    final float qrankNorm;
    final float meta;
    final float eleNorm;
    final long qrankRaw;

    Result(float prominence, float base, float qrankNorm, float meta, float eleNorm, long qrankRaw) {
      this.prominence = prominence;
      this.base = base;
      this.qrankNorm = qrankNorm;
      this.meta = meta;
      this.eleNorm = eleNorm;
      this.qrankRaw = qrankRaw;
    }
  }

  /**
   * @param naturalTag  value of the OSM natural tag (peak/spring/...), or null
   * @param placeTag    value of the OSM place tag (city/town/village/...), or null
   * @param boundaryTag value of the OSM boundary tag (national_park/protected_area/...), or null
   * @param tourismTag  value of the OSM tourism tag (viewpoint/attraction/...), or null
   * @param historicTag value of the OSM historic tag, or null
   * @param waterwayTag value of the OSM waterway tag, or null
   * @param ele         parsed elevation in meters, or Double.NaN if absent
   * @param hasImage    image / wikimedia_commons tag present
   * @param hasWebsite  website tag present
   * @param hasWikidata wikidata tag present (proxy for "documented in wikipedia/wikidata")
   * @param qrankRaw    raw QRank value (0 if absent/unknown)
   */
  static Result compute(String naturalTag, String placeTag, String boundaryTag, String tourismTag,
      String historicTag, String waterwayTag, double ele, boolean hasImage, boolean hasWebsite,
      boolean hasWikidata, long qrankRaw) {

    double eleNorm = Double.isNaN(ele) ? 0.0 : clamp01(Math.log1p(Math.max(0, ele)) / Math.log1p(MAX_ELE_M));
    double base = baseScore(naturalTag, placeTag, boundaryTag, tourismTag, historicTag, waterwayTag, eleNorm);

    double qnorm = (qrankRaw <= 1) ? 0.0 : clamp01(Math.log(qrankRaw) / Math.log(QRANK_REF));

    double meta = clamp01(0.40 * (hasImage ? 1 : 0) + 0.35 * (hasWebsite ? 1 : 0) + 0.25 * (hasWikidata ? 1 : 0));

    double prom = clamp01(FLOOR + 0.45 * base + 0.40 * qnorm + 0.10 * meta);

    return new Result((float) prom, (float) base, (float) qnorm, (float) meta, (float) eleNorm, qrankRaw);
  }

  /** Feature-class prior in [0,1]. Highest signal wins (a place that is also a peak gets the peak rule). */
  private static double baseScore(String natural, String place, String boundary, String tourism,
      String historic, String waterway, double eleNorm) {
    if ("peak".equals(natural) || "volcano".equals(natural)) {
      return 0.30 + 0.55 * eleNorm; // elevation-aware: a 4000m peak >> a 200m hill
    }
    if (place != null) {
      switch (place) {
        case "city":      return 1.00;
        case "town":      return 0.80;
        case "village":   return 0.55;
        case "hamlet":    return 0.35;
        default:          return 0.45; // suburb/neighbourhood/locality/...
      }
    }
    if ("national_park".equals(boundary) || "protected_area".equals(boundary)) {
      return 0.80;
    }
    if ("viewpoint".equals(tourism) || "attraction".equals(tourism) || "alpine_hut".equals(tourism)) {
      return 0.55;
    }
    if (historic != null) {
      return 0.55; // ruins, archaeological_site, monument, memorial, castle...
    }
    if ("spring".equals(natural) || "hot_spring".equals(natural) || "cave_entrance".equals(natural)
        || waterway != null) {
      return 0.30;
    }
    return 0.25; // generic named node baseline
  }

  static double clamp01(double v) {
    if (v < 0) return 0;
    if (v > 1) return 1;
    return v;
  }
}
