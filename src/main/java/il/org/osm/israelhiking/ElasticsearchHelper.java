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

  static final String HEBREW_VAV = "ו";
  static final String HEBREW_YOD = "י";
  static final String HEBREW_DOUBLED_VAV_PATTERN = "\\u05D5\\u05D5";
  static final String HEBREW_DOUBLED_YOD_PATTERN = "\\u05D9\\u05D9";
  static final String HEBREW_NIQQUD_PATTERN = "[\\u05B0-\\u05C7]";

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
            .refreshInterval(t -> t.time("-1")) // avoid refresh while building
            .numberOfReplicas("0") // reduce coping while building
            .analysis(a -> a
                .charFilter("hebrew_niqqud", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern(HEBREW_NIQQUD_PATTERN)
                            .replacement("")))) // remove niqqud comletely
                .charFilter("hebrew_matres_vav", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern(HEBREW_DOUBLED_VAV_PATTERN)
                            .replacement(HEBREW_VAV)))) // replace double וו with single ו
                .charFilter("hebrew_matres_yod", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern(HEBREW_DOUBLED_YOD_PATTERN)
                            .replacement(HEBREW_YOD)))) // replace double יי with single י
                .filter("edge_ngram_2_15", tf -> tf
                    .definition(d -> d
                        .edgeNgram(en -> en.minGram(2).maxGram(15)))) // for prefix match
                .normalizer("universal_normalizer", n -> n
                    .custom(cn -> cn
                        .charFilter("hebrew_niqqud")
                        .filter("asciifolding", "lowercase")))
                .normalizer("hebrew_normalizer", n -> n
                    .custom(cn -> cn
                        .charFilter("hebrew_niqqud", "hebrew_matres_vav", "hebrew_matres_yod")
                        .filter("asciifolding", "lowercase")))
                .analyzer("universal_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))
                .analyzer("hebrew_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud", "hebrew_matres_vav", "hebrew_matres_yod")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))
                .analyzer("prefix_index_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase", "edge_ngram_2_15")))
                .analyzer("prefix_search_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))
                .analyzer("hebrew_prefix_index_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud", "hebrew_matres_vav", "hebrew_matres_yod")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase", "edge_ngram_2_15")))
                .analyzer("hebrew_prefix_search_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud", "hebrew_matres", "hebrew_matres_yod")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))))
        .mappings(m -> {
          for (var lang : allLanguages) {
            var isHebrew = "he".equals(lang); // Hebrew fields get a diffent configuration
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
            m.properties("alt_names." + lang, k -> k
                .text(p -> p
                    .analyzer(isHebrew ? "hebrew_analyzer" : "universal_analyzer")
                    .fields("keyword", f -> f
                        .keyword(kw -> kw
                            .normalizer(isHebrew ? "hebrew_normalizer" : "universal_normalizer")))));
          }
          m.properties("location", g -> g.geoPoint(p -> p));
          m.properties("poiProminence", n -> n.float_(f -> f));
          m.properties("population", n -> n.integer(f -> f));
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
            .refreshInterval(t -> t.time("-1")) // avoid refresh while building
            .numberOfReplicas("0") // reduce coping while building
            .analysis(a -> a
                .charFilter("hebrew_niqqud", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern(HEBREW_NIQQUD_PATTERN)
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
    boolean aliasExists = esClient.indices().existsAlias(c -> c.name(indexAlias)).value();
    esClient.indices().updateAliases(c -> {
      if (aliasExists) {
        c.actions(a -> a.remove(i -> i.index("*").alias(indexAlias)));
      }
      return c.actions(a -> a.add(c2 -> c2.index(targetIndex).alias(indexAlias)));
    });
  }

  /**
   * Restore normal search-time settings before an index goes live: 
   * re-enable periodic refresh and add one replica (createPointsIndex/createBBoxIndex disable both for faster bulk writes). 
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
