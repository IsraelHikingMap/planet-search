package il.org.osm.israelhiking;

final class ProminenceCalculator {

  static final double QRANK_REF = 3_000_000.0;
  static final double MAX_ELE_M = 8849.0;
  /** Never zero, so the query-time multiply can't annihilate a hit. */
  static final double FLOOR = 0.05;

  private ProminenceCalculator() {}

  static float compute(String featureClass, double ele, boolean hasImage, boolean hasWebsite,
      boolean hasWikidata, long qrankRaw) {

    double eleNorm = Double.isNaN(ele) ? 0.0 : clamp01(Math.log1p(Math.max(0, ele)) / Math.log1p(MAX_ELE_M));
    double base = basePrior(featureClass, eleNorm);
    double qnorm = (qrankRaw <= 1) ? 0.0 : clamp01(Math.log(qrankRaw) / Math.log(QRANK_REF));
    double meta = clamp01(0.40 * (hasImage ? 1 : 0) + 0.35 * (hasWebsite ? 1 : 0) + 0.25 * (hasWikidata ? 1 : 0));

    return (float) clamp01(FLOOR + 0.45 * base + 0.40 * qnorm + 0.10 * meta);
  }

  private static double basePrior(String featureClass, double eleNorm) {
    if (featureClass == null) {
      return 0.25;
    }
    switch (featureClass) {
      case "peak":            return 0.30 + 0.55 * eleNorm;
      case "city":            return 1.00;
      case "town":            return 0.80;
      case "village":         return 0.55;
      case "hamlet":          return 0.35;
      case "suburb": case "neighbourhood": case "island": case "locality": case "place":
                              return 0.45;
      case "park":            return 0.80;
      case "viewpoint": case "attraction": case "lodging":
                              return 0.55;
      case "historic":        return 0.55;
      case "spring": case "cave":
      case "river": case "stream": case "canal": case "waterfall": case "rapids": case "waterway":
                              return 0.30;
      default:                return 0.25;
    }
  }

  static double clamp01(double v) {
    if (Double.isNaN(v)) return 0; // Math.min/max pass NaN through; keep it out of the ES score
    if (v < 0) return 0;
    if (v > 1) return 1;
    return v;
  }
}
