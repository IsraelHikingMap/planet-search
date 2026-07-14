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
import java.util.Set;
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

  private final ObjectMapper mapper = new ObjectMapper();
  private final HttpClient http = HttpClient.newBuilder()
      .connectTimeout(TIMEOUT)
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build();

  @TestFactory
  Stream<DynamicTest> searchRelevance() throws Exception {
    String endpoint = System.getProperty("relevance.endpoint", DEFAULT_ENDPOINT).replaceAll("/+$", "");
    return SearchCases.load("/search-relevance-cases.json").stream().map(c -> DynamicTest.dynamicTest(
        c.id() + " · " + c.searchTerm() + " (" + c.uiLanguage() + ")",
        () -> assertTimeoutPreemptively(CASE_TIMEOUT, () -> assertNearTarget(endpoint, c))));
  }

  @AfterAll
  void closeHttp() {
    http.close();
  }

  private void assertNearTarget(String endpoint, Case searchCase) throws Exception {
    var failure = SearchCases.failure(searchCase, search(endpoint, searchCase));
    if (failure != null) {
      fail(failure);
    }
  }

  private List<Hit> search(String endpoint, Case searchCase) throws Exception {
    String term = URLEncoder.encode(searchCase.searchTerm(), StandardCharsets.UTF_8).replace("+", "%20");
    String url = endpoint + "/api/search/" + term + "?language="
        + URLEncoder.encode(searchCase.uiLanguage(), StandardCharsets.UTF_8);
    if (searchCase.hasCenter()) {
      url += "&lat=" + searchCase.center().get(0) + "&lng=" + searchCase.center().get(1)
          + "&zoom=" + (searchCase.zoom() == null ? 12 : searchCase.zoom());
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
}
