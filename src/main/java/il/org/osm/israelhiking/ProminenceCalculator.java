package il.org.osm.israelhiking;

final class ProminenceCalculator {

  static final double MAX_ELE_M = 8849.0;
  static final double PROMINENCE_FLOOR = 0.05;
  static final double WEIGHT_BASE = 0.45;
  static final double WEIGHT_WIKIPEDIA = 0.30;
  static final double WEIGHT_META = 0.10;

  private ProminenceCalculator() {}

  /**
   * How well known a feature is, from 0 to 1, out of its own tags only.
   *
   * A wikipedia tag is the fame signal: a feature with an encyclopedia article
   * is the one a searcher means when an obscure feature shares its name, such
   * as Denali the peak over Denali Law Group, or the John Muir Trail over the
   * John Muir Trail Cabin. Wikidata only counts towards the metadata, since
   * wikidata items are also created in bulk for features nobody looks up.
   */
  static float compute(OsmFeatureClassifier.Category category, double ele, boolean hasImage,
      boolean hasWebsite, boolean hasWikidata, boolean hasWikipedia) {

    double eleNormalized = Double.isNaN(ele) ? 0.0 : clamp01(Math.log1p(Math.max(0, ele)) / Math.log1p(MAX_ELE_M));
    double base = clamp01(category.baseScore + category.elevationWeight * eleNormalized);
    double meta = clamp01(0.40 * (hasImage ? 1 : 0) + 0.35 * (hasWebsite ? 1 : 0) + 0.25 * (hasWikidata ? 1 : 0));

    return (float) clamp01(PROMINENCE_FLOOR + WEIGHT_BASE * base + WEIGHT_WIKIPEDIA * (hasWikipedia ? 1 : 0)
        + WEIGHT_META * meta);
  }

  static double clamp01(double v) {
    if (Double.isNaN(v)) return 0;
    if (v < 0) return 0;
    if (v > 1) return 1;
    return v;
  }
}
