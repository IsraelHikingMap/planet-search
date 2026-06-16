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

  /**
   * Return the number of documents in the index the given alias currently resolves to (the live /
   * served index), or 0 if the alias does not exist or the cluster cannot be probed. Counts against
   * the alias, not getTargetIndexName (the build writes to the inactive target, which is ~0).
   *
   * A missing alias yields 0 so the first-ever build is never blocked. But if the alias exists and
   * its doc-count read fails, do NOT fail open to 0 (that would let a transient error defeat the
   * guard) — rethrow so assertSafeToReindex blocks the reindex (fail closed).
   */
  public static long getLiveAliasDocCount(ElasticsearchClient esClient, String indexAlias) {
    boolean aliasExists;
    try {
      aliasExists = esClient.indices().existsAlias(c -> c.name(indexAlias)).value();
    } catch (Exception e) {
      // Can't even tell whether the alias exists (cluster unreachable / not yet up). Treat as a
      // fresh/absent cluster — fail OPEN to 0 so the first-ever build is never blocked.
      LOGGER.warning("Safety guard: could not probe alias '" + indexAlias + "' existence ("
          + e.getClass().getSimpleName() + ": " + e.getMessage() + "); treating live doc-count as 0.");
      return 0L;
    }

    if (!aliasExists) {
      LOGGER.info("Safety guard: alias '" + indexAlias
          + "' does not exist yet — treating live doc-count as 0 (fresh build).");
      return 0L;
    }

    // Alias EXISTS → there may be live data behind it. A failure to COUNT it must NOT be read as
    // "0 docs" (that would defeat the guard). Fail CLOSED: rethrow so the reindex is blocked.
    try {
      long count = esClient.count(c -> c.index(indexAlias)).count();
      LOGGER.info("Safety guard: alias '" + indexAlias + "' currently resolves to "
          + count + " live document(s).");
      return count;
    } catch (Exception e) {
      throw new IllegalStateException("Safety guard: alias '" + indexAlias + "' exists but its "
          + "live doc-count could not be read (" + e.getClass().getSimpleName() + ": " + e.getMessage()
          + "). Refusing to assume 0 and reindex over possibly-live data — pass --force-reindex to override.", e);
    }
  }

  /**
   * Refuse to destroy a populated, live index unless an intentional reindex was requested. Throws
   * before any index is deleted/recreated when the live doc-count is at/above minProtectDocs and
   * forceReindex is false.
   *
   * @param esClient       the ES client
   * @param indexAlias     the alias whose live index must be protected (e.g. "points")
   * @param minProtectDocs threshold at/above which the live index is protected (default 1_000_000)
   * @param forceReindex   true to bypass the guard for a legitimate rebuild
   * @throws IllegalStateException if the live index is protected and force was not set
   */
  public static void assertSafeToReindex(ElasticsearchClient esClient, String indexAlias,
      long minProtectDocs, boolean forceReindex) {
    // Check the override before reading the live count: a forced rebuild bypasses the guard, so a
    // count read-error must not block a legitimate force build.
    if (forceReindex) {
      LOGGER.warning("Safety guard: --force-reindex set — bypassing live-index protection for alias '"
          + indexAlias + "'.");
      return;
    }
    long liveDocs = getLiveAliasDocCount(esClient, indexAlias);
    if (liveDocs >= minProtectDocs) {
      throw new IllegalStateException(String.join("\n",
          "",
          "============================================================================",
          "  REFUSING TO REINDEX: a populated Elasticsearch index is protected.",
          "============================================================================",
          "  Alias '" + indexAlias + "' currently serves " + liveDocs + " live document(s),",
          "  which is at or above the protection threshold of " + minProtectDocs + ".",
          "",
          "  This indexer DELETES and RECREATES its target index on every run. Proceeding",
          "  now would have DESTROYED the live data above (this is exactly the incident",
          "  where a stray 'docker compose up site' wiped the whole-planet index down to a",
          "  small regional one).",
          "",
          "  If this is an INTENTIONAL rebuild (e.g. a real 'make prod' planet build),",
          "  re-run with:   --force-reindex",
          "  (or set the argument force-reindex=true / FORCE_REINDEX env via config).",
          "",
          "  To tune the protection threshold use:   --min-protect-docs=<N>",
          "============================================================================",
          ""));
    }
    LOGGER.info("Safety guard: alias '" + indexAlias + "' has " + liveDocs
        + " live document(s) (below threshold " + minProtectDocs + ") — safe to reindex.");
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
                // INDEX analyzer for the *.prefix subfield: universal pipeline + edge_ngram.
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
          // Ranking signals (additive; docs without these use missing:1.0 at query time).
          m.properties("prominence", n -> n.float_(f -> f));   // hot path: field_value_factor
          m.properties("population", n -> n.integer(f -> f));  // place/admin layer
          // Coarse feature type ("peak"/"lake"/"city"...) for class-match ranking; keyword for exact
          // term match with doc_values for query-time comparison.
          m.properties("feature_class", n -> n.keyword(f -> f));
          // Enrichment signals (index:false query-time scoring inputs; doc_values readable by script).
          m.properties("area_norm", n -> n.float_(f -> f.index(false)));
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

  /**
   * Live document statistics used by the post-build reconcile gate. count is live (non-deleted)
   * docs; deleted is soft-deleted docs pending merge. A re-emitted id overwrites and turns the old
   * version into a deleted doc, so count + deleted approximates the distinct index ops applied,
   * which is what we compare against emitted points.
   */
  public record IndexDocsStats(long count, long deleted) {
    public long countPlusDeleted() {
      return count + deleted;
    }
  }

  /**
   * Read live doc-count + deleted-count for the index the given alias resolves to, via the indices
   * stats API (so docs.deleted is available). Returns null when the alias can't be probed, so the
   * caller can fail open rather than block a build on a transient stats hiccup.
   */
  public static IndexDocsStats getLiveAliasDocsStats(ElasticsearchClient esClient, String indexAlias) {
    try {
      var resp = esClient.indices().stats(s -> s.index(indexAlias));
      // Prefer the rolled-up "all" view; fall back to summing per-index entries.
      if (resp.all() != null && resp.all().primaries() != null && resp.all().primaries().docs() != null) {
        var docs = resp.all().primaries().docs();
        long deleted = docs.deleted() == null ? 0L : docs.deleted();
        return new IndexDocsStats(docs.count(), deleted);
      }
      long count = 0L;
      long deleted = 0L;
      for (var entry : resp.indices().values()) {
        if (entry.primaries() != null && entry.primaries().docs() != null) {
          count += entry.primaries().docs().count();
          deleted += entry.primaries().docs().deleted() == null ? 0L : entry.primaries().docs().deleted();
        }
      }
      return new IndexDocsStats(count, deleted);
    } catch (Exception e) {
      LOGGER.warning("Reconcile gate: could not read docs-stats for alias '" + indexAlias
          + "' (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ").");
      return null;
    }
  }

  /**
   * Decide whether a freshly-built, now-live points index is acceptably complete. Pure (no I/O) so
   * it is directly unit-testable. Expected live docs = emittedPoints - genuineDataFailures
   * (transient charges are NOT subtracted: ES likely committed them after the client timed out).
   * Compares against count + deleted (distinct ops landed), not the bare count, so dedup overwrites
   * don't look like loss. Fails when the shortfall exceeds shortfallTolerance (e.g. 0.001 = 0.1%).
   *
   * @return null when within tolerance; otherwise a human-readable failure message.
   */
  public static String reconcileLivePoints(long emittedPoints, long genuineDataFailures,
      IndexDocsStats stats, double shortfallTolerance) {
    if (stats == null) {
      // Could not probe — fail OPEN (don't block on a stats hiccup); caller logs.
      return null;
    }
    long expected = Math.max(0L, emittedPoints - genuineDataFailures);
    long landed = stats.countPlusDeleted();
    long allowedShortfall = (long) Math.ceil(expected * shortfallTolerance);
    long shortfall = expected - landed;
    if (shortfall > allowedShortfall) {
      return String.join("\n",
          "",
          "============================================================================",
          "  RECONCILE GATE FAILED: the live points index is short of expected.",
          "============================================================================",
          "  Emitted points          : " + emittedPoints,
          "  Genuine data failures    : " + genuineDataFailures + " (subtracted from expected)",
          "  Expected to be present   : " + expected,
          "  Live docs (count)        : " + stats.count(),
          "  Deleted docs (overwrites): " + stats.deleted(),
          "  Landed (count+deleted)   : " + landed,
          "  Shortfall                : " + shortfall
              + "  (allowed up to " + allowedShortfall + " = "
              + (shortfallTolerance * 100.0) + "%)",
          "",
          "  More documents were emitted than ended up in the live index than the",
          "  tolerance permits. This indicates real, silent document loss — refusing",
          "  to treat the index as complete.",
          "============================================================================",
          "");
    }
    return null;
  }
}
