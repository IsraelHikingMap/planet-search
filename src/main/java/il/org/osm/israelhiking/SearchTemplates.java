package il.org.osm.israelhiking;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

/**
 * The search queries that the query side runs against the indices this repo
 * builds. They are stored in Elasticsearch as mustache search templates, so
 * that the query and the index it assumes are built, versioned and tested
 * together here, and the caller only needs to send the search parameters.
 *
 * The template files are the queries themselves, with the relevance tuning
 * constants written into them, and they use two kinds of placeholders:
 * <ul>
 * <li>{@code [[#languages]]...[[lang]]...[[/languages]]} is expanded here, when
 * the template is rendered, into one clause per supported language.</li>
 * <li>{@code {{...}}} is left as is, and is expanded by Elasticsearch on every
 * search with the parameters the caller sends.</li>
 * </ul>
 */
public final class SearchTemplates {

  public static final String POINTS_SEARCH = "points_search";
  public static final String POINTS_SEARCH_EXACT = "points_search_exact";
  public static final String BBOX_CONTAINER = "bbox_container";
  public static final String BBOX_CONTAINS = "bbox_contains";

  /** The parameters each template accepts, published in the search contract. */
  public static final Map<String, List<String>> PARAMETERS = Map.of(
      POINTS_SEARCH, List.of("searchTerm", "prefix", "hasCenter", "lat", "lng", "zoom",
          "hasPlaceShape", "placeShape"),
      POINTS_SEARCH_EXACT, List.of("searchTerm"),
      BBOX_CONTAINER, List.of("place", "prefix"),
      BBOX_CONTAINS, List.of("shape"));

  private static final List<String> ALL = List.of(POINTS_SEARCH, POINTS_SEARCH_EXACT,
      BBOX_CONTAINER, BBOX_CONTAINS);

  private static final Pattern LANGUAGES_BLOCK = Pattern.compile(
      "\\[\\[#languages\\]\\](.*?)\\[\\[/languages\\]\\]", Pattern.DOTALL);

  /** Static utility class should not be instantiated. */
  private SearchTemplates() {
  }

  /**
   * Stores every search template in Elasticsearch, so that they are swapped
   * together with the index they were built for.
   */
  public static void register(ElasticsearchClient esClient, String[] allLanguages) throws IOException {
    for (var templateId : ALL) {
      var source = render(templateId, allLanguages);
      esClient.putScript(p -> p
          .id(templateId)
          .script(s -> s
              .lang("mustache")
              .source(source)));
    }
  }

  /**
   * @return every template, rendered, keyed by its id - used by the search
   *         contract and by the tests.
   */
  public static Map<String, String> renderAll(String[] allLanguages) {
    var templates = new LinkedHashMap<String, String>();
    for (var templateId : ALL) {
      templates.put(templateId, render(templateId, allLanguages));
    }
    return templates;
  }

  /**
   * Reads a template and repeats every [[#languages]] block once per language,
   * joined by a comma, so that a block turns into a JSON list of per language
   * clauses.
   */
  public static String render(String templateId, String[] allLanguages) {
    var matcher = LANGUAGES_BLOCK.matcher(read(templateId));
    var result = new StringBuilder();
    while (matcher.find()) {
      var block = matcher.group(1);
      var expanded = Arrays.stream(allLanguages)
          .map(language -> block.replace("[[lang]]", language))
          .collect(Collectors.joining(","));
      matcher.appendReplacement(result, Matcher.quoteReplacement(expanded));
    }
    matcher.appendTail(result);
    return result.toString();
  }

  private static String read(String templateId) {
    var resource = "/search-templates/" + templateId + ".json";
    try (InputStream stream = SearchTemplates.class.getResourceAsStream(resource)) {
      if (stream == null) {
        throw new IllegalStateException("Missing search template: " + resource);
      }
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read search template: " + resource, e);
    }
  }
}
