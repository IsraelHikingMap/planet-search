package il.org.osm.israelhiking;

final class ProminenceCalculator {

  static final double QRANK_REF = 3_000_000.0;
  static final double MAX_ELE_M = 8849.0;
  static final double FLOOR = 0.05;

  private ProminenceCalculator() {}

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

  static Result compute(String naturalTag, String placeTag, String boundaryTag, String tourismTag,
      String historicTag, String waterwayTag, double ele, boolean hasImage, boolean hasWebsite,
      boolean hasWikidata, long qrankRaw) {

    double eleNorm = Double.isNaN(ele) ? 0.0 : clamp01(Math.log1p(Math.max(0, ele)) / Math.log1p(MAX_ELE_M));
    double base = baseScore(naturalTag, placeTag, boundaryTag, tourismTag, historicTag, waterwayTag, eleNorm);

    double qnorm = (!hasWikidata || qrankRaw <= 1) ? 0.0 : clamp01(Math.log(qrankRaw) / Math.log(QRANK_REF));

    double meta = clamp01(0.40 * (hasImage ? 1 : 0) + 0.35 * (hasWebsite ? 1 : 0) + 0.25 * (hasWikidata ? 1 : 0));

    double prom = clamp01(FLOOR + 0.45 * base + 0.40 * qnorm + 0.10 * meta);

    return new Result((float) prom, (float) base, (float) qnorm, (float) meta, (float) eleNorm, qrankRaw);
  }

  private static double baseScore(String natural, String place, String boundary, String tourism,
      String historic, String waterway, double eleNorm) {
    if ("peak".equals(natural) || "volcano".equals(natural)) {
      return 0.30 + 0.55 * eleNorm;
    }
    if (place != null) {
      switch (place) {
        case "city":      return 1.00;
        case "town":      return 0.80;
        case "village":   return 0.55;
        case "hamlet":    return 0.35;
        default:          return 0.45;
      }
    }
    if ("national_park".equals(boundary) || "protected_area".equals(boundary)) {
      return 0.80;
    }
    if ("viewpoint".equals(tourism) || "attraction".equals(tourism) || "alpine_hut".equals(tourism)) {
      return 0.55;
    }
    if (historic != null) {
      return 0.55;
    }
    if ("spring".equals(natural) || "hot_spring".equals(natural) || "cave_entrance".equals(natural)
        || waterway != null) {
      return 0.30;
    }
    return 0.25;
  }

  static double clamp01(double v) {
    if (v < 0) return 0;
    if (v > 1) return 1;
    return v;
  }
}
