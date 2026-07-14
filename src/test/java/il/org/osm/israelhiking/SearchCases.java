package il.org.osm.israelhiking;

import java.io.InputStream;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The search cases both search tests are driven by: a search term and the place
 * it is expected to find, i.e. a target coordinate, a radius around it and how
 * deep in the results to look for it.
 * The end to end test runs them against an index it builds and the search
 * templates, and the relevance test runs them against the search API of a
 * running site, so they only share the cases themselves and what makes a case
 * pass.
 */
public final class SearchCases {

  private static final double EARTH_RADIUS_METERS = 6_371_000.0;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * @param prefix   whether the search is an autocomplete one, optional
   * @param template which search template to use, optional, the end to end test
   *                 only
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Case(String id, String searchTerm, String uiLanguage, List<Double> center, Integer zoom,
      Boolean prefix, String template, List<Double> expectedTarget, double radiusMeters, int topN) {

    public boolean hasCenter() {
      return center != null && center.size() >= 2;
    }

    public boolean isPrefix() {
      return Boolean.TRUE.equals(prefix);
    }
  }

  /** A search result, whatever it was searched with. */
  public record Hit(String title, double lat, double lng) {
  }

  /** Static utility class should not be instantiated. */
  private SearchCases() {
  }

  public static List<Case> load(String resource) throws Exception {
    try (InputStream stream = SearchCases.class.getResourceAsStream(resource)) {
      if (stream == null) {
        throw new IllegalStateException("missing classpath resource: " + resource);
      }
      List<Case> cases = MAPPER.readValue(stream,
          MAPPER.getTypeFactory().constructCollectionType(List.class, Case.class));
      cases.forEach(SearchCases::validate);
      return cases;
    }
  }

  private static void validate(Case searchCase) {
    boolean valid = searchCase.searchTerm() != null && searchCase.uiLanguage() != null
        && searchCase.expectedTarget() != null && searchCase.expectedTarget().size() >= 2
        && searchCase.topN() >= 1 && searchCase.radiusMeters() > 0;
    if (!valid) {
      throw new IllegalStateException("invalid search case: " + searchCase.id());
    }
  }

  /**
   * A case passes when one of its top hits is within the radius of the place it
   * was expected to find.
   *
   * @return null when it passes, and what was found instead when it does not.
   */
  public static String failure(Case searchCase, List<Hit> hits) {
    var closest = Double.POSITIVE_INFINITY;
    Hit closestHit = null;
    for (var hit : hits.subList(0, Math.min(searchCase.topN(), hits.size()))) {
      var distance = distanceInMeters(hit.lat(), hit.lng(),
          searchCase.expectedTarget().get(0), searchCase.expectedTarget().get(1));
      if (!Double.isNaN(distance) && distance < closest) {
        closest = distance;
        closestHit = hit;
      }
    }
    if (closest <= searchCase.radiusMeters()) {
      return null;
    }
    var found = closestHit == null
        ? "nothing"
        : String.format("\"%s\" at %.0f m", closestHit.title(), closest);
    return String.format("%s: \"%s\" (%s) - no hit in the top %d within %.0f m, the closest was %s",
        searchCase.id(), searchCase.searchTerm(), searchCase.uiLanguage(), searchCase.topN(),
        searchCase.radiusMeters(), found);
  }

  public static double distanceInMeters(double lat1, double lng1, double lat2, double lng2) {
    var deltaLat = Math.toRadians(lat2 - lat1);
    var deltaLng = Math.toRadians(lng2 - lng1);
    var a = Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2)
        + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
            * Math.sin(deltaLng / 2) * Math.sin(deltaLng / 2);
    return 2 * EARTH_RADIUS_METERS * Math.asin(Math.sqrt(a));
  }
}
