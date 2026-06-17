package il.org.osm.israelhiking;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class ElasticsearchHelper {
  /**
   * Static utility class should not be instantiated.
   */
  private ElasticsearchHelper() {
  }

  private static final Logger LOGGER = Logger.getLogger(ElasticsearchHelper.class.getName());

  // Hebrew matres-lectionis, doubled-only fold (he-scoped). The PATTERN strings are regex text sent
  // verbatim to ES (Java "\\u" => one backslash, which ES/Lucene parses as a unicode escape); the
  // replacement is a single Hebrew code point. Collapse a doubled vav/yod to a single one; never
  // drop a single interior vav/yod (that fuller rule would merge ~7 real homographs).
  static final String HEBREW_VAV = "ו";
  static final String HEBREW_YOD = "י";
  static final String HEBREW_DOUBLED_VAV_PATTERN = "\\u05D5\\u05D5";
  static final String HEBREW_DOUBLED_YOD_PATTERN = "\\u05D9\\u05D9";

  /**
   * Pure reference implementation of the doubled-only matres rule, so a unit test can assert it
   * collapses doubled vav/yod but leaves single (and other) letters untouched without a live ES.
   */
  static String applyHebrewMatresDoubledOnly(String input) {
    if (input == null) {
      return null;
    }
    return input
        .replaceAll("וו", HEBREW_VAV)
        .replaceAll("יי", HEBREW_YOD);
  }

  // Socket timeout 180s (not the 30s default) so a whole-planet bulk-fill survives long ES pauses
  // without charging committed batches as failures; connect timeout stays short so a down cluster
  // fails fast. Units are milliseconds.
  public static ElasticsearchClient createElasticsearchClient(String esAddress) {
    Logger.getLogger("org.elasticsearch.client.RestClient").setLevel(Level.OFF);
    RestClient restClient = RestClient.builder(HttpHost.create(esAddress))
        .setRequestConfigCallback(rc -> rc
            .setConnectTimeout(10_000)
            .setSocketTimeout(180_000))
        .build();
    ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    return new ElasticsearchClient(transport);
  }

  public static String createPointsIndex(ElasticsearchClient esClient, String indexAlias,
      String[] supportedLanguages) throws Exception {
    var targetIndex = getTargetIndexName(indexAlias, esClient);
    if (esClient.indices().exists(c -> c.index(targetIndex)).value()) {
      esClient.indices().delete(c -> c.index(targetIndex));
    }
    var allLanguages = Stream.concat(Stream.of("default"), Arrays.stream(supportedLanguages))
        .toArray(String[]::new);
    esClient.indices().create(c -> c.index(targetIndex)
        .settings(s -> s
            // Build-time write tuning: the target index isn't served until the alias swaps, so
            // disabling periodic refresh and replicas removes the biggest bulk-indexing overhead.
            // Restored right before the alias swap.
            .refreshInterval(t -> t.time("-1"))
            .numberOfReplicas("0")
            .analysis(a -> a
                .charFilter("hebrew_niqqud", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern("[\\u05B0-\\u05C7]")
                            .replacement(""))))
                // Hebrew matres-lectionis fold, doubled-only: collapse doubled vav (וו -> ו) and
                // doubled yod (יי -> י). Deliberately does NOT drop a single interior vav/yod
                // (that fuller rule would merge ~7 real homographs). Applied only to the he-scoped
                // analyzer/normalizer so the blast radius stays in Hebrew. The pattern is a regex
                // (two vav/yod code points); the replacement is a literal single Hebrew glyph.
                .charFilter("hebrew_matres", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern(HEBREW_DOUBLED_VAV_PATTERN)
                            .replacement(HEBREW_VAV))))
                .charFilter("hebrew_matres_yod", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern(HEBREW_DOUBLED_YOD_PATTERN)
                            .replacement(HEBREW_YOD))))
                // Edge-ngram index-side token filter: emit 2..15-char edge n-grams so a prefix
                // matches a stored gram in O(1) term lookups, independent of match_phrase_prefix's
                // max_expansions cap.
                .filter("edge_ngram_2_15", tf -> tf
                    .definition(d -> d
                        .edgeNgram(en -> en.minGram(2).maxGram(15))))
                .normalizer("universal_normalizer", n -> n
                    .custom(cn -> cn
                        .charFilter("hebrew_niqqud")
                        .filter("asciifolding", "lowercase")))
                // he-scoped keyword normalizer = universal_normalizer + the doubled-matres folds.
                .normalizer("hebrew_normalizer", n -> n
                    .custom(cn -> cn
                        .charFilter("hebrew_niqqud", "hebrew_matres", "hebrew_matres_yod")
                        .filter("asciifolding", "lowercase")))
                .analyzer("universal_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))
                // Hebrew-scoped text analyzer = universal_analyzer + doubled-only matres folds.
                // Used on name.he / alt_names.he only.
                .analyzer("hebrew_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud", "hebrew_matres", "hebrew_matres_yod")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))
                .analyzer("prefix_index_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase", "edge_ngram_2_15")))
                // SEARCH analyzer for the *.prefix subfield: same pipeline minus edge_ngram, so the
                // query term is not itself exploded into grams (otherwise "ba" would match anything
                // sharing a 2-gram).
                .analyzer("prefix_search_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))
                // He-scoped prefix analyzers = the prefix analyzers above + the doubled-matres
                // folds, so name.he's .prefix edge-grams fold doubled vav/yod the same way the main
                // name.he field (hebrew_analyzer) does. Without these, a Hebrew as-you-type prefix
                // query would recall differently from the full-token query the matres fold added.
                .analyzer("hebrew_prefix_index_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud", "hebrew_matres", "hebrew_matres_yod")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase", "edge_ngram_2_15")))
                .analyzer("hebrew_prefix_search_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud", "hebrew_matres", "hebrew_matres_yod")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))))
        .mappings(m -> {
          for (var lang : allLanguages) {
            // name.he gets the Hebrew-scoped analyzer (doubled-matres collapse); every other
            // language keeps universal_analyzer. The .prefix subfield (edge-ngram) is added on all
            // languages for cap-independent prefix recall.
            var isHebrew = "he".equals(lang);
            m.properties("name." + lang, k -> k
                .text(p -> p
                    .analyzer(isHebrew ? "hebrew_analyzer" : "universal_analyzer")
                    .fields("keyword", f -> f
                        .keyword(kw -> kw
                            .normalizer(isHebrew ? "hebrew_normalizer" : "universal_normalizer")))
                    .fields("prefix", f -> f
                        .text(pt -> pt
                            .analyzer(isHebrew ? "hebrew_prefix_index_analyzer" : "prefix_index_analyzer")
                            .searchAnalyzer(isHebrew ? "hebrew_prefix_search_analyzer" : "prefix_search_analyzer")))));
            // alt_names.lang — separate demoted variant-name field, not folded into name (folding
            // breaks ranking/display). Same analyzer choice as name so he variants get the matres
            // collapse too. No .prefix subfield.
            m.properties("alt_names." + lang, k -> k
                .text(p -> p
                    .analyzer(isHebrew ? "hebrew_analyzer" : "universal_analyzer")
                    .fields("keyword", f -> f
                        .keyword(kw -> kw
                            .normalizer(isHebrew ? "hebrew_normalizer" : "universal_normalizer")))));
          }
          m.properties("location", g -> g.geoPoint(p -> p));
          // Computed ranking signals (additive; docs without these use missing:1.0 at query time).
          // poi* prefix marks them as calculated, not raw OSM tags.
          m.properties("poiProminence", n -> n.float_(f -> f));   // hot path: field_value_factor
          m.properties("population", n -> n.integer(f -> f));     // place/admin layer
          // Coarse feature type ("peak"/"lake"/"city"...) for class-match ranking; keyword for exact
          // term match with doc_values for query-time comparison.
          m.properties("poiFeatureClass", n -> n.keyword(f -> f));
          // Enrichment signals (index:false query-time scoring inputs; doc_values readable by script).
          m.properties("poiAreaNorm", n -> n.float_(f -> f.index(false)));
          m.properties("intermittent", n -> n.boolean_(f -> f.index(false)));
          return m;
        }));

    return targetIndex;
  }

  public static String createBBoxIndex(ElasticsearchClient esClient, String indexAlias,
      String[] supportedLanguages) throws Exception {
    var targetIndex = getTargetIndexName(indexAlias, esClient);
    if (esClient.indices().exists(c -> c.index(targetIndex)).value()) {
      esClient.indices().delete(c -> c.index(targetIndex));
    }
    var allLanguages = Stream.concat(Stream.of("default"), Arrays.stream(supportedLanguages))
        .toArray(String[]::new);
    esClient.indices().create(c -> c.index(targetIndex)
        .settings(s -> s
            // Build-time write tuning (see createPointsIndex) — restored before
            // the alias swap.
            .refreshInterval(t -> t.time("-1"))
            .numberOfReplicas("0")
            .analysis(a -> a
                .charFilter("hebrew_niqqud", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern("[\\u05B0-\\u05C7]")
                            .replacement(""))))
                .normalizer("universal_normalizer", n -> n
                    .custom(cn -> cn
                        .charFilter("hebrew_niqqud")
                        .filter("asciifolding",
                            "lowercase")))
                .analyzer("universal_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))))
        .mappings(m -> {
          for (var lang : allLanguages) {
            m.properties("name." + lang, k -> k
                .text(p -> p
                    .analyzer("universal_analyzer")
                    .fields("keyword", f -> f
                        .keyword(kw -> kw
                            .normalizer("universal_normalizer")))));
          }
          m.properties("bbox", g -> g.geoShape(p -> p));
          m.properties("area", n -> n.float_(f -> f));
          m.properties("center", g -> g.geoPoint(p -> p));
          return m;
        }));

    return targetIndex;
  }

  private static String getTargetIndexName(String indexAlias, ElasticsearchClient esClient) throws Exception {
    var indexName = indexAlias + "1";
    if (!esClient.indices().existsAlias(c -> c.name(indexAlias)).value()) {
      return indexName;
    }
    var alias = esClient.indices().getAlias(c -> c.name(indexAlias)).result();
    if (alias.containsKey(indexName)) {
      return indexAlias + "2";
    }
    return indexName;
  }

  public static void switchAlias(ElasticsearchClient esClient, String indexAlias, String targetIndex)
      throws Exception {
    // On the first-ever build no index carries the alias yet. updateAliases is atomic, and a
    // remove action for an alias that exists on no index can be rejected ("aliases [X] missing"),
    // which would fail the whole request and leave the freshly built index unaliased. So only
    // issue the remove when the alias actually exists; otherwise add-only.
    boolean aliasExists = esClient.indices().existsAlias(c -> c.name(indexAlias)).value();
    esClient.indices().updateAliases(c -> {
      if (aliasExists) {
        c.actions(a -> a.remove(i -> i.index("*").alias(indexAlias)));
      }
      return c.actions(a -> a.add(c2 -> c2.index(targetIndex).alias(indexAlias)));
    });
  }

  /**
   * Restore normal search-time settings before an index goes live: re-enable periodic refresh and
   * add one replica (createPointsIndex/createBBoxIndex disable both for faster bulk writes). Call
   * after the final flush and refresh, just before switchAlias.
   */
  public static void restoreSearchSettings(ElasticsearchClient esClient, String targetIndex)
      throws Exception {
    esClient.indices().putSettings(p -> p
        .index(targetIndex)
        .settings(s -> s
            .refreshInterval(t -> t.time("1s"))
            .numberOfReplicas("1")));
  }
}
