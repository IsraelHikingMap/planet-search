package il.org.osm.israelhiking;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.IndexSettingsAnalysis;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class ElasticsearchHelper {

  // Doubled-only: folding a single vav/yod would merge real homographs. "\\u"
  // reaches ES as the literal Lucene escape.
  static final String HEBREW_VAV = "ו";
  static final String HEBREW_YOD = "י";
  static final String HEBREW_DOUBLED_VAV_PATTERN = "\\u05D5\\u05D5";
  static final String HEBREW_DOUBLED_YOD_PATTERN = "\\u05D9\\u05D9";
  static final String NIQQUD_PATTERN = "[\\u05B0-\\u05C7]";
  static final List<String> HEBREW_CHAR_FILTERS = List.of("hebrew_niqqud", "hebrew_matres_vav", "hebrew_matres_yod");

  public static record ElasticRunContext(
      ElasticsearchClient esClient,
      String pointsIndexAlias,
      String bboxIndexAlias,
      String pointsIndexTarget,
      String bboxIndexTarget,
      String[] supportedLanguages,
      QRankLookup qrankLookup,
      BulkIndexer bulkListener,
      ContainerIndex containerIndex) {
  }

  /**
   * Static utility class should not be instantiated.
   */
  private ElasticsearchHelper() {
  }

  /**
   * @return the languages of the index, i.e. the supported languages and the
   *         "default" one, which holds the unsuffixed name of a feature.
   */
  public static String[] allLanguages(String[] supportedLanguages) {
    return Stream.concat(Stream.of("default"), Arrays.stream(supportedLanguages)).toArray(String[]::new);
  }

  public static ElasticsearchClient createElasticsearchClient(String esAddress) {
    Logger.getLogger("org.elasticsearch.client.RestClient").setLevel(Level.OFF);
    RestClient restClient = RestClient.builder(HttpHost.create(esAddress)).build();
    ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    return new ElasticsearchClient(transport);
  }

  /**
   * Registers the char filters, normalizer and analyzer that are shared by all
   * the indices.
   * The Hebrew char filters are applied to every language and not only to
   * Hebrew fields: they only touch Hebrew characters, so they are a no-op for
   * Latin, Cyrillic and Arabic text, while making sure a Hebrew name is
   * normalized the same way no matter which language field it ended up in.
   */
  private static IndexSettingsAnalysis.Builder addCommonAnalysis(IndexSettingsAnalysis.Builder analysis) {
    return analysis
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
        .normalizer("universal_normalizer", n -> n
            .custom(cn -> cn
                .charFilter(HEBREW_CHAR_FILTERS)
                .filter("asciifolding", "lowercase")))
        .analyzer("universal_analyzer", an -> an
            .custom(ca -> ca
                .charFilter(HEBREW_CHAR_FILTERS)
                .tokenizer("standard")
                .filter("asciifolding", "lowercase")));
  }

  public static String createPointsIndex(ElasticsearchClient esClient, String indexAlias,
      String[] supportedLanguages) throws Exception {
    var targetIndex = getTargetIndexName(indexAlias, esClient);
    if (esClient.indices().exists(c -> c.index(targetIndex)).value()) {
      esClient.indices().delete(c -> c.index(targetIndex));
    }
    var allLanguages = allLanguages(supportedLanguages);
    esClient.indices().create(c -> c.index(targetIndex)
        .settings(s -> s
            .analysis(a -> addCommonAnalysis(a)
                .filter("edge_ngram_2_15", tf -> tf
                    .definition(d -> d
                        .edgeNgram(en -> en.minGram(2).maxGram(15))))
                .analyzer("prefix_index_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter(HEBREW_CHAR_FILTERS)
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase", "edge_ngram_2_15")))
                .analyzer("prefix_search_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter(HEBREW_CHAR_FILTERS)
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))))
        .mappings(m -> {
          for (var lang : allLanguages) {
            m.properties("name." + lang, k -> k
                .text(p -> p
                    .analyzer("universal_analyzer")
                    .fields("keyword", f -> f
                        .keyword(kw -> kw
                            .normalizer("universal_normalizer")))
                    .fields("prefix", f -> f
                        .text(pt -> pt
                            .analyzer("prefix_index_analyzer")
                            .searchAnalyzer("prefix_search_analyzer")))));
            m.properties("alt_names." + lang, k -> k
                .text(p -> p
                    .analyzer("universal_analyzer")
                    .fields("keyword", f -> f
                        .keyword(kw -> kw
                            .normalizer("universal_normalizer")))
                    .fields("prefix", f -> f
                        .text(pt -> pt
                            .analyzer("prefix_index_analyzer")
                            .searchAnalyzer("prefix_search_analyzer")))));
            m.properties("poiParentNames." + lang, k -> k
                .text(p -> p.analyzer("universal_analyzer")));
            m.properties("poiContainer." + lang, k -> k.keyword(kw -> kw));
            m.properties("poiCountry." + lang, k -> k.keyword(kw -> kw));
          }
          m.properties("location", g -> g.geoPoint(p -> p));
          m.properties("poiProminence", n -> n.float_(f -> f));
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
    var allLanguages = allLanguages(supportedLanguages);
    esClient.indices().create(c -> c.index(targetIndex)
        .settings(s -> s
            .analysis(a -> addCommonAnalysis(a)))
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
          m.properties("adminLevel", n -> n.integer(f -> f));
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
      BulkIndexer bulkListener,
      String pointsIndexAlias,
      String bboxIndexAlias,
      String[] supportedLanguages,
      QRankLookup qrankLookup,
      ContainerIndex containerIndex) throws Exception {
    var targetPointsIndex = ElasticsearchHelper.createPointsIndex(esClient, pointsIndexAlias,
        supportedLanguages);
    var targetBBoxIndex = ElasticsearchHelper.createBBoxIndex(esClient, bboxIndexAlias, supportedLanguages);
    return new ElasticRunContext(esClient, pointsIndexAlias, bboxIndexAlias, targetPointsIndex, targetBBoxIndex,
        supportedLanguages, qrankLookup, bulkListener, containerIndex);
  }

  /**
   * Points the aliases at the indices of this run and stores the search
   * templates that query them, so that the query side always runs the queries
   * that were built for the live index.
   */
  public static void finalizeRun(ElasticRunContext context) throws Exception {
    context.bulkListener().close();

    context.esClient().indices().refresh(r -> r.index(context.pointsIndexTarget(), context.bboxIndexTarget()));

    SearchTemplates.register(context.esClient(), allLanguages(context.supportedLanguages()));

    ElasticsearchHelper.switchAlias(context.esClient(), context.pointsIndexAlias(), context.pointsIndexTarget());
    ElasticsearchHelper.switchAlias(context.esClient(), context.bboxIndexAlias(), context.bboxIndexTarget());
  }
}
