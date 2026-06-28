package il.org.osm.israelhiking.relevance;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("relevance")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SearchRelevanceTest {

  @JsonIgnoreProperties(ignoreUnknown = true)
  record Case(String id, String searchTerm, String uiLanguage,
      List<Double> center, int zoom, List<Double> expectedTarget,
      double radiusMeters, int topN) {
  }

  private record Hit(String title, double lat, double lng) {
  }

  private static final String DEFAULT_ENDPOINT = "https://mapeak.com";
  private static final Duration TIMEOUT = Duration.ofSeconds(20);
  private static final double EARTH_R_M = 6_371_000.0;
  private static final int MAX_ATTEMPTS = 3;
  private static final long RETRY_BACKOFF_MS = 2_000;
  private static final int MAX_BODY_CHARS = 4_000_000;
  private static final Duration CASE_TIMEOUT = Duration.ofSeconds(90);
  private static final Set<Integer> RETRYABLE_STATUS = Set.of(429, 502, 503, 504);

  private final ObjectMapper mapper = new ObjectMapper();
  private final HttpClient http = HttpClient.newBuilder()
      .connectTimeout(TIMEOUT)
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build();

  @TestFactory
  Stream<DynamicTest> searchRelevance() throws Exception {
    String endpoint = System.getProperty("relevance.endpoint", DEFAULT_ENDPOINT).replaceAll("/+$", "");
    InputStream stream = getClass().getResourceAsStream("/search-relevance-cases.json");
    if (stream == null) {
      throw new IllegalStateException("missing classpath resource: /search-relevance-cases.json");
    }
    List<Case> cases = mapper.readValue(
        stream,
        mapper.getTypeFactory().constructCollectionType(List.class, Case.class));
    cases.forEach(SearchRelevanceTest::validate);

    return cases.stream().map(c -> DynamicTest.dynamicTest(
        c.id() + " · " + c.searchTerm() + " (" + c.uiLanguage() + ")",
        () -> assertTimeoutPreemptively(CASE_TIMEOUT, () -> assertNearTarget(endpoint, c))));
  }

  @AfterAll
  void closeHttp() {
    http.close();
  }

  private static void validate(Case c) {
    boolean ok = c.searchTerm() != null && c.uiLanguage() != null
        && c.center() != null && c.center().size() >= 2
        && c.expectedTarget() != null && c.expectedTarget().size() >= 2
        && c.topN() >= 1 && c.radiusMeters() > 0;
    if (!ok) {
      throw new IllegalStateException("invalid gold case: " + c.id());
    }
  }

  private void assertNearTarget(String endpoint, Case c) throws Exception {
    List<Hit> hits = search(endpoint, c);
    int limit = Math.min(Math.max(c.topN(), 1), hits.size());

    double best = Double.POSITIVE_INFINITY;
    Hit bestHit = null;
    for (int i = 0; i < limit; i++) {
      Hit h = hits.get(i);
      double d = haversineMeters(h.lat(), h.lng(),
          c.expectedTarget().get(0), c.expectedTarget().get(1));
      if (!Double.isNaN(d) && d < best) {
        best = d;
        bestHit = h;
      }
    }

    if (best > c.radiusMeters()) {
      String closest = bestHit == null
          ? "none (no results)"
          : String.format("\"%s\" at %.0f m", bestHit.title(), best);
      fail(String.format(
          "\"%s\" (%s): no hit in top %d within %.0f m of target — closest %s",
          c.searchTerm(), c.uiLanguage(), c.topN(), c.radiusMeters(), closest));
    }
  }

  private List<Hit> search(String endpoint, Case c) throws Exception {
    String term = URLEncoder.encode(c.searchTerm(), StandardCharsets.UTF_8).replace("+", "%20");
    String url = endpoint + "/api/search/" + term + "?language="
        + URLEncoder.encode(c.uiLanguage(), StandardCharsets.UTF_8);
    if (c.center() != null && c.center().size() >= 2) {
      url += "&lat=" + c.center().get(0) + "&lng=" + c.center().get(1) + "&zoom=" + c.zoom();
    }

    HttpRequest req = HttpRequest.newBuilder(URI.create(url))
        .timeout(TIMEOUT)
        .header("User-Agent", "planet-search-relevance/1.0")
        .GET()
        .build();
    HttpResponse<String> res = sendWithRetry(req, url);
    if (res.statusCode() / 100 != 2) {
      throw new RuntimeException("HTTP " + res.statusCode() + " for " + url);
    }
    String body = res.body();
    if (body.length() > MAX_BODY_CHARS) {
      throw new RuntimeException("response too large (" + body.length() + " chars) for " + url);
    }

    JsonNode arr = mapper.readTree(body);
    if (!arr.isArray()) {
      throw new RuntimeException("expected a JSON array of hits, got " + arr.getNodeType());
    }
    List<Hit> hits = new ArrayList<>();
    for (JsonNode hit : arr) {
      JsonNode loc = hit.path("location");
      double lat = loc.path("lat").isNumber() ? loc.path("lat").asDouble() : Double.NaN;
      double lng = loc.path("lng").isNumber() ? loc.path("lng").asDouble() : Double.NaN;
      if (Math.abs(lat) > 90 || Math.abs(lng) > 180) {
        lat = Double.NaN;
        lng = Double.NaN;
      }
      String title = hit.hasNonNull("title") ? hit.path("title").asText()
          : hit.path("displayName").asText("");
      hits.add(new Hit(title, lat, lng));
    }
    return hits;
  }

  private HttpResponse<String> sendWithRetry(HttpRequest req, String url) throws Exception {
    Exception last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (!RETRYABLE_STATUS.contains(res.statusCode())) {
          return res;
        }
        last = new RuntimeException("HTTP " + res.statusCode() + " for " + url);
      } catch (IOException e) {
        last = e;
      }
      if (attempt < MAX_ATTEMPTS) {
        Thread.sleep(RETRY_BACKOFF_MS * attempt);
      }
    }
    throw last;
  }

  private static double haversineMeters(double lat1, double lng1, double lat2, double lng2) {
    double dPhi = Math.toRadians(lat2 - lat1);
    double dLambda = Math.toRadians(lng2 - lng1);
    double h = Math.sin(dPhi / 2) * Math.sin(dPhi / 2)
        + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
        * Math.sin(dLambda / 2) * Math.sin(dLambda / 2);
    return 2 * EARTH_R_M * Math.asin(Math.sqrt(h));
  }
}
