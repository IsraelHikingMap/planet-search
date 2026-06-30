package il.org.osm.israelhiking;

final class ProminenceCalculator {

  static final double QRANK_SCALE = 3_000_000.0;
  static final double MAX_ELE_M = 8849.0;
  static final double PROMINENCE_FLOOR = 0.05;
  static final double WEIGHT_BASE = 0.45;
  static final double WEIGHT_QRANK = 0.40;
  static final double WEIGHT_META = 0.10;

  private ProminenceCalculator() {}

  static float compute(OsmFeatureClassifier.Category category, double ele, boolean hasImage,
      boolean hasWebsite, boolean hasWikidata, long qrankRaw) {

    double eleNormalized = Double.isNaN(ele) ? 0.0 : clamp01(Math.log1p(Math.max(0, ele)) / Math.log1p(MAX_ELE_M));
    double base = clamp01(category.baseScore + category.elevationWeight * eleNormalized);
    double qrankNormalized = (!hasWikidata || qrankRaw <= 1) ? 0.0 : clamp01(Math.log(qrankRaw) / Math.log(QRANK_SCALE));
    double meta = clamp01(0.40 * (hasImage ? 1 : 0) + 0.35 * (hasWebsite ? 1 : 0) + 0.25 * (hasWikidata ? 1 : 0));

    return (float) clamp01(PROMINENCE_FLOOR + WEIGHT_BASE * base + WEIGHT_QRANK * qrankNormalized + WEIGHT_META * meta);
  }

  static double clamp01(double v) {
    if (Double.isNaN(v)) return 0;
    if (v < 0) return 0;
    if (v > 1) return 1;
    return v;
  }
}
