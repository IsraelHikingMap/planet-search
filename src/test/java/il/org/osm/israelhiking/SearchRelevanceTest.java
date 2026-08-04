package il.org.osm.israelhiking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;

import il.org.osm.israelhiking.SearchCases.Case;
import il.org.osm.israelhiking.SearchCases.Hit;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Scores the search of a running site against the gold cases.
 * It shares the cases and what makes a case pass with the end to end test, and
 * only searches differently: through the search API of the site, and not
 * against an index it built itself.
 */
@Tag("relevance")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SearchRelevanceTest {

  private static final String DEFAULT_ENDPOINT = "https://mapeak.com";
  private static final Duration TIMEOUT = Duration.ofSeconds(20);
  private static final int MAX_ATTEMPTS = 3;
  private static final long RETRY_BACKOFF_MS = 2_000;
  private static final int MAX_BODY_CHARS = 4_000_000;
  private static final Duration CASE_TIMEOUT = Duration.ofSeconds(90);
  private static final Set<Integer> RETRYABLE_STATUS = Set.of(429, 502, 503, 504);
  private static final int DEFAULT_ZOOM = 12;
  private static final int TARGET_LINK_ZOOM = 15;
  /** The site puts one zoom level more in its URL than the one it searches with. */
  private static final int URL_ZOOM_OFFSET = 1;

  private final ObjectMapper mapper = new ObjectMapper();
  private final HttpClient http = HttpClient.newBuilder()
      .connectTimeout(TIMEOUT)
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build();

  @TestFactory
  Stream<DynamicTest> searchRelevance() throws Exception {
    String endpoint = System.getProperty("relevance.endpoint", DEFAULT_ENDPOINT).replaceAll("/+$", "");
    var cases = SearchCases.load("/search-relevance-cases.json");
    var done = new AtomicInteger();
    return cases.stream().map(c -> DynamicTest.dynamicTest(
        c.id() + " · " + c.searchTerm() + " (" + c.uiLanguage() + ")",
        () -> {
          var startedNanos = System.nanoTime();
          var hits = assertTimeoutPreemptively(CASE_TIMEOUT, () -> search(endpoint, c));
          var failure = SearchCases.failure(c, hits.stream().map(apiHit -> apiHit.hit()).toList());
          var details = failure == null ? "" : "\n" + failure + links(endpoint, c, hits);
          // Everything a failure needs is printed as it happens, so that a run
          // can be debugged while the rest of the cases are still going.
          System.out.println(String.format(Locale.ROOT, "[%3d/%d] %s %5.1fs  %s · \"%s\" (%s)",
              done.incrementAndGet(), cases.size(), failure == null ? "PASS" : "FAIL",
              (System.nanoTime() - startedNanos) / 1_000_000_000.0,
              c.id(), c.searchTerm(), c.uiLanguage()) + details);
          if (failure != null) {
            fail(details.strip());
          }
        }));
  }

  @AfterAll
  void closeHttp() {
    http.close();
  }

  /**
   * Links to look at a failing case in the browser: where it searched, where it
   * expected to find something, the API call itself and the entities it did
   * find. The site takes the search term from its own search box, so the map
   * links only put the map where the search ran.
   */
  private String links(String endpoint, Case searchCase, List<ApiHit> hits) {
    var links = new StringBuilder();
    if (searchCase.hasCenter()) {
      links.append("\n    searched at: ").append(mapLink(endpoint, searchCase.center().get(0),
          searchCase.center().get(1), zoom(searchCase)));
    }
    links.append("\n    expected at: ").append(mapLink(endpoint, searchCase.expectedTarget().get(0),
        searchCase.expectedTarget().get(1), TARGET_LINK_ZOOM));
    links.append("\n    api:         ").append(searchUrl(endpoint, searchCase));
    var top = hits.subList(0, Math.min(searchCase.topN(), hits.size()));
    for (var i = 0; i < top.size(); i++) {
      links.append(i == 0 ? "\n    top hits:    " : "\n                 ")
          .append(poiLink(endpoint, searchCase, top.get(i)))
          .append(" - \"").append(top.get(i).hit().title()).append('"');
    }
    return links.toString();
  }

  private String mapLink(String endpoint, double lat, double lng, int zoom) {
    return String.format(Locale.ROOT, "%s/map/%d/%.5f/%.5f", endpoint, zoom + URL_ZOOM_OFFSET, lat, lng);
  }

  private String poiLink(String endpoint, Case searchCase, ApiHit apiHit) {
    return endpoint + "/poi/" + apiHit.source() + "/" + apiHit.hit().id() + "?language="
        + URLEncoder.encode(searchCase.uiLanguage(), StandardCharsets.UTF_8);
  }

  private String searchUrl(String endpoint, Case searchCase) {
    String term = URLEncoder.encode(searchCase.searchTerm(), StandardCharsets.UTF_8).replace("+", "%20");
    String url = endpoint + "/api/search/" + term + "?language="
        + URLEncoder.encode(searchCase.uiLanguage(), StandardCharsets.UTF_8);
    if (searchCase.hasCenter()) {
      url += "&lat=" + searchCase.center().get(0) + "&lng=" + searchCase.center().get(1)
          + "&zoom=" + zoom(searchCase);
    }
    if (searchCase.isPrefix()) {
      url += "&prefix=true";
    }
    return url;
  }

  private int zoom(Case searchCase) {
    return searchCase.zoom() == null ? DEFAULT_ZOOM : searchCase.zoom();
  }

  /** A hit and the source the site returns next to it, which its POI links need. */
  private record ApiHit(Hit hit, String source) {
  }

  private List<ApiHit> search(String endpoint, Case searchCase) throws Exception {
    String url = searchUrl(endpoint, searchCase);
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
    List<ApiHit> hits = new ArrayList<>();
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
      hits.add(new ApiHit(new Hit(hit.path("id").asText(null), title, lat, lng),
          hit.path("source").asText("OSM")));
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
}
