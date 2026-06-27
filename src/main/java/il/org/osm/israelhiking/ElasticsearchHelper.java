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

  private static final Logger LOGGER = Logger.getLogger(ElasticsearchHelper.class.getName());

  // Doubled-only: folding a single vav/yod would merge real homographs. "\\u"
  // reaches ES as the literal Lucene escape.
  static final String HEBREW_VAV = "ו";
  static final String HEBREW_YOD = "י";
  static final String HEBREW_DOUBLED_VAV_PATTERN = "\\u05D5\\u05D5";
  static final String HEBREW_DOUBLED_YOD_PATTERN = "\\u05D9\\u05D9";
  static final String NIQQUD_PATTERN = "[\\u05B0-\\u05C7]";

  public static record ElasticRunContext(
      ElasticsearchClient esClient,
      String pointsIndexAlias,
      String bboxIndexAlias,
      String pointsIndexTarget,
      String bboxIndexTarget,
      String[] supportedLanguages,
      QRankIndex qrankIndex) {
  }

  /**
   * Static utility class should not be instantiated.
   */
  private ElasticsearchHelper() {
  }

  public static ElasticsearchClient createElasticsearchClient(String esAddress) {
    Logger.getLogger("org.elasticsearch.client.RestClient").setLevel(Level.OFF);
    RestClient restClient = RestClient.builder(HttpHost.create(esAddress)).build();
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
            .refreshInterval(t -> t.time("-1"))
            .numberOfReplicas("0")
            .analysis(a -> a
                .charFilter("hebrew_niqqud", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern(NIQQUD_PATTERN)
                            .replacement(""))))
                .charFilter("hebrew_matres_vav", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern(HEBREW_DOUBLED_VAV_PATTERN)
                            .replacement(HEBREW_VAV))))
                .charFilter("hebrew_matres_yod", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern(HEBREW_DOUBLED_YOD_PATTERN)
                            .replacement(HEBREW_YOD))))
                .filter("edge_ngram_2_15", tf -> tf
                    .definition(d -> d
                        .edgeNgram(en -> en.minGram(2).maxGram(15))))
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
                        .charFilter("hebrew_niqqud", "hebrew_matres_vav", "hebrew_matres_yod")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))))
        .mappings(m -> {
          for (var lang : allLanguages) {
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
            m.properties("alt_names." + lang, k -> k
                .text(p -> p
                    .analyzer(isHebrew ? "hebrew_analyzer" : "universal_analyzer")
                    .fields("keyword", f -> f
                        .keyword(kw -> kw
                            .normalizer(isHebrew ? "hebrew_normalizer" : "universal_normalizer")))
                    .fields("prefix", f -> f
                        .text(pt -> pt
                            .analyzer(isHebrew ? "hebrew_prefix_index_analyzer" : "prefix_index_analyzer")
                            .searchAnalyzer(isHebrew ? "hebrew_prefix_search_analyzer" : "prefix_search_analyzer")))));
          }
          m.properties("location", g -> g.geoPoint(p -> p));
          m.properties("poiProminence", n -> n.float_(f -> f));
          m.properties("poiPromBase", n -> n.float_(f -> f.index(false)));
          m.properties("poiPromQrankNorm", n -> n.float_(f -> f.index(false)));
          m.properties("poiPromMeta", n -> n.float_(f -> f.index(false)));
          m.properties("poiEleNorm", n -> n.float_(f -> f.index(false)));
          m.properties("poiQrankRaw", n -> n.long_(f -> f.index(false)));
          m.properties("population", n -> n.integer(f -> f));
          m.properties("poiFeatureClass", n -> n.keyword(f -> f));
          m.properties("poiAreaNormalized", n -> n.float_(f -> f.index(false)));
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
            .refreshInterval(t -> t.time("-1"))
            .numberOfReplicas("0")
            .analysis(a -> a
                .charFilter("hebrew_niqqud", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern(NIQQUD_PATTERN)
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

  public static ElasticRunContext initRun(ElasticsearchClient esClient,
      String pointsIndexAlias,
      String bboxIndexAlias,
      String[] supportedLanguages,
      QRankIndex qrankIndex) throws Exception {
    var targetPointsIndex = ElasticsearchHelper.createPointsIndex(esClient, pointsIndexAlias,
        supportedLanguages);
    var targetBBoxIndex = ElasticsearchHelper.createBBoxIndex(esClient, bboxIndexAlias, supportedLanguages);
    return new ElasticRunContext(esClient, pointsIndexAlias, bboxIndexAlias, targetPointsIndex, targetBBoxIndex,
        supportedLanguages, qrankIndex);
  }

  public static void restoreSearchSettings(ElasticsearchClient esClient, String targetIndex)
      throws Exception {
    esClient.indices().putSettings(p -> p
        .index(targetIndex)
        .settings(s -> s
            .refreshInterval(t -> t.time("1s"))
            .numberOfReplicas("1")));
  }

  public static void finalizeRun(ElasticRunContext context, PlanetSearchProfile profile) throws Exception {
    profile.flush();
    context.esClient().indices().refresh(r -> r.index(context.pointsIndexTarget(), context.bboxIndexTarget()));

    restoreSearchSettings(context.esClient(), context.pointsIndexTarget());
    restoreSearchSettings(context.esClient(), context.bboxIndexTarget());

    ElasticsearchHelper.switchAlias(context.esClient(), context.bboxIndexAlias(), context.bboxIndexTarget());

    // A partial points index must never go live; leave the alias on the previous complete index.
    if (profile.hasIndexingFailures()) {
      LOGGER.warning(() -> "Leaving points alias '" + context.pointsIndexAlias()
          + "' on the previous index: this build dropped " + profile.getFailedPointsCount()
          + " points document(s), so '" + context.pointsIndexTarget()
          + "' is incomplete and must not go live.");
      return;
    }
    ElasticsearchHelper.switchAlias(context.esClient(), context.pointsIndexAlias(), context.pointsIndexTarget());
  }
}
