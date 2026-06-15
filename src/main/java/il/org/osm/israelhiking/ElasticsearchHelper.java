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

  // --- ADR-0011 item 2: Hebrew matres-lectionis, DOUBLED-ONLY rule (he-scoped) -----------------
  // The PATTERN strings are the regex text sent verbatim to ES (Java "\\u" => one backslash + "u",
  // which ES/Lucene parses as a unicode escape). The replacement is a single Hebrew code point.
  // DOUBLED-ONLY: collapse a doubled vav/yod to a single one; NEVER drop a single interior vav/yod
  // (that fuller rule fixes HV01/HV02 but merges ~7 homographs — deferred for client sign-off).
  static final String HEBREW_VAV = "ו"; // ו
  static final String HEBREW_YOD = "י"; // י
  static final String HEBREW_DOUBLED_VAV_PATTERN = "\\u05D5\\u05D5";
  static final String HEBREW_DOUBLED_YOD_PATTERN = "\\u05D9\\u05D9";

  /**
   * PURE reference implementation of the doubled-only matres rule, so a unit test can assert it
   * collapses doubled vav/yod but leaves single (and other) letters untouched — without a live ES.
   * Applies the SAME pattern/replacement constants the index char_filters use. Note the patterns
   * carry a doubled backslash for ES's regex parser; in plain Java regex one backslash addresses a
   * unicode escape, so the test compiles the de-escaped form (see HebrewMatresRuleTest).
   */
  static String applyHebrewMatresDoubledOnly(String input) {
    if (input == null) {
      return null;
    }
    // Java's own \\uXXXX in a regex literal == the code point; collapse doubled -> single.
    return input
        .replaceAll("וו", HEBREW_VAV)
        .replaceAll("יי", HEBREW_YOD);
  }

  public static ElasticsearchClient createElasticsearchClient(String esAddress) {
    Logger.getLogger("org.elasticsearch.client.RestClient").setLevel(Level.OFF);
    RestClient restClient = RestClient.builder(HttpHost.create(esAddress))
        // Step D — indexer resilience: the default per-request socket timeout is 30s,
        // which is too tight for a whole-planet bulk-fill on a constrained box under
        // merge/GC pressure. A single ES pause longer than 30s made the client give up
        // and report "30,000 milliseconds timeout on connection", charging whole bulk
        // batches as failures even though ES had (likely) committed them. Raise the
        // socket (read) timeout to 180s; keep the connect timeout short (10s) so a
        // genuinely-down cluster still fails fast. Units are milliseconds.
        .setRequestConfigCallback(rc -> rc
            .setConnectTimeout(10_000)
            .setSocketTimeout(180_000))
        .build();
    ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    return new ElasticsearchClient(transport);
  }

  /**
   * Return the number of documents in the index the given alias currently
   * resolves to (the LIVE / served index), or 0 if the alias does not exist or
   * the cluster cannot be probed.
   *
   * <p>This counts against the alias name itself, so Elasticsearch resolves it to
   * whatever underlying index is live right now (points1 or points2). It must NOT
   * be confused with {@link #getTargetIndexName} — the build always writes to the
   * INACTIVE target index, so counting the target would always report ~0 and would
   * never protect anything. The guard cares about the live data the alias serves.
   *
   * <p>Robustness: a missing alias (fresh cluster / first ever build) yields 0 so the very first
   * build is never blocked. But if the alias EXISTS and its doc-count read fails, we DO NOT fail
   * open to 0 — that would let a transient read error defeat the guard and wipe a populated live
   * index. In that case we rethrow so {@link #assertSafeToReindex} blocks the reindex (fail CLOSED).
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
   * Refuse to destroy a populated, live index unless an intentional reindex was
   * requested. Computes the LIVE (currently-aliased) doc-count and, if it is at or
   * above {@code minProtectDocs} while {@code forceReindex} is false, throws before
   * any index is deleted/recreated.
   *
   * <p>This is the root fix for the incident where {@code docker compose up site}
   * pulled the indexer with its default {@code area=us/colorado} and reindexed OVER
   * the live whole-planet index, dropping it from 22.8M docs to ~83k.
   *
   * @param esClient       the ES client
   * @param indexAlias     the alias whose live index must be protected (e.g. "points")
   * @param minProtectDocs threshold at/above which the live index is considered
   *                       "populated" and protected (default 1_000_000)
   * @param forceReindex   true to bypass the guard for a legitimate rebuild
   * @throws IllegalStateException if the live index is protected and force was not set
   */
  public static void assertSafeToReindex(ElasticsearchClient esClient, String indexAlias,
      long minProtectDocs, boolean forceReindex) {
    // Check the override BEFORE reading the live count: a forced rebuild bypasses the guard
    // entirely, so a count read-error must not block a legitimate `make prod` force build.
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
            // Build-time write tuning: the index being built lives under the
            // "2" suffix and isn't served until the alias swaps, so nobody
            // searches it during the build. Disabling periodic refresh and
            // replicas removes the biggest per-document overhead in bulk
            // indexing. They are restored right before the alias swap.
            .refreshInterval(t -> t.time("-1"))
            .numberOfReplicas("0")
            .analysis(a -> a
                .charFilter("hebrew_niqqud", cf -> cf
                    .definition(d -> d
                        .patternReplace(pr -> pr
                            .pattern("[\\u05B0-\\u05C7]")
                            .replacement(""))))
                // Step E / ADR-0011 item 2 — Hebrew matres-lectionis normalization, DOUBLED-ONLY
                // (conservative). Collapses a doubled vav (וו -> ו) and a doubled yod (יי -> י)
                // ONLY. It deliberately does NOT drop a SINGLE interior vav/yod: that fuller rule
                // would fix the client's exact אופניים/אפניים case (HV01/HV02) but it also merges
                // ~7 real homographs (אור/אר, דוד/דד, סוף/סף...), so it is deferred for client
                // sign-off. Doubled-only is the safe, ~zero-homograph-break win. Applied ONLY to
                // the he-scoped analyzer/normalizer below (NOT the shared universal ones) so the
                // blast radius is contained to Hebrew. ו = vav (ו), י = yod (י).
                // NOTE on escaping: the PATTERN is a regex sent to ES as the literal text
                // "\\u05D5\\u05D5" (Java "\\" => one backslash), which ES/Lucene's regex engine
                // parses as two vav code points. The REPLACEMENT is a literal (NOT regex), so it
                // must carry the ACTUAL Hebrew character (the vav/yod glyph itself, single code
                // point). Pattern + replacement are pulled from the package-private constants below
                // so HebrewMatresRuleTest exercises the same doubled-only rule.
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
                // Edge-ngram INDEX-side token filter (ADR-0010 opt D): emit 2..15-char edge n-grams
                // so a prefix matches a stored gram in O(1) term lookups — recall is independent of
                // match_phrase_prefix's max_expansions cap.
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
                // SEARCH analyzer for the *.prefix subfield: SAME pipeline MINUS edge_ngram, so the
                // query term is NOT itself exploded into grams (otherwise "ba" would match anything
                // sharing a 2-gram). This index-vs-search split is the whole point of opt D.
                .analyzer("prefix_search_analyzer", an -> an
                    .custom(ca -> ca
                        .charFilter("hebrew_niqqud")
                        .tokenizer("standard")
                        .filter("asciifolding", "lowercase")))))
        .mappings(m -> {
          for (var lang : allLanguages) {
            // name.he gets the Hebrew-scoped analyzer (doubled-matres collapse); every other
            // language keeps universal_analyzer. The .prefix subfield (edge-ngram) is added on all
            // languages for cap-independent prefix recall (ADR-0010 opt D).
            var isHebrew = "he".equals(lang);
            m.properties("name." + lang, k -> k
                .text(p -> p
                    .analyzer(isHebrew ? "hebrew_analyzer" : "universal_analyzer")
                    .fields("keyword", f -> f
                        .keyword(kw -> kw
                            .normalizer(isHebrew ? "hebrew_normalizer" : "universal_normalizer")))
                    .fields("prefix", f -> f
                        .text(pt -> pt
                            .analyzer("prefix_index_analyzer")
                            .searchAnalyzer("prefix_search_analyzer")))));
            // alt_names.<lang> — SEPARATE demoted variant-name field (ADR-0011 item 1). NOT folded
            // into name (folding breaks ranking/display/ADR-0009). Same analyzer choice as name so
            // he variants get the matres collapse too. No .prefix subfield (alt prefix is out of
            // this bundle's query design).
            m.properties("alt_names." + lang, k -> k
                .text(p -> p
                    .analyzer(isHebrew ? "hebrew_analyzer" : "universal_analyzer")
                    .fields("keyword", f -> f
                        .keyword(kw -> kw
                            .normalizer(isHebrew ? "hebrew_normalizer" : "universal_normalizer")))));
          }
          m.properties("location", g -> g.geoPoint(p -> p));
          // Ranking signals (additive — existing queries are unaffected; docs without
          // these fields use missing:1.0 at query time).
          m.properties("prominence", n -> n.float_(f -> f));   // hot path: field_value_factor
          m.properties("population", n -> n.integer(f -> f));  // place/admin layer
          // Coarse feature type ("peak"/"lake"/"city"...) for class-match ranking. keyword: exact
          // term match only (no analysis), low cardinality, doc_values for query-time comparison.
          m.properties("feature_class", n -> n.keyword(f -> f));
          // Raw components kept for re-tuning weights without a reindex: not searchable
          // (index:false) but doc_values stay on so they remain readable.
          m.properties("prom_base", n -> n.float_(f -> f.index(false)));
          m.properties("prom_qrank_norm", n -> n.float_(f -> f.index(false)));
          m.properties("prom_meta", n -> n.float_(f -> f.index(false)));
          m.properties("ele_norm", n -> n.float_(f -> f.index(false)));
          m.properties("qrank_raw", n -> n.long_(f -> f.index(false)));
          // ADR-0014 enrichment signals (all index:false — query-time scoring inputs, re-weighted
          // in the script without a reindex; doc_values stay on so the script can read them).
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
    esClient.indices().updateAliases(c -> c.actions(a -> a.remove(i -> i.index("*").alias(indexAlias)))
        .actions(a -> a.add(c2 -> c2.index(targetIndex).alias(indexAlias))));
  }

  /**
   * Restore normal search-time settings on a freshly-built index before it goes
   * live: re-enable periodic refresh (createPointsIndex/createBBoxIndex disable
   * it for faster bulk writes) and add one replica for redundancy. Call this
   * after the final flush and a one-off refresh, just before switchAlias.
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
   * Live document statistics for an index/alias used by the post-build reconcile
   * gate. {@code count} is the number of live (non-deleted) documents; {@code deleted}
   * is the number of soft-deleted docs still pending merge. During a build that
   * re-emits the same id (dedup overwrites), the OLD version becomes a deleted doc,
   * so {@code count + deleted} approximates the number of distinct index operations
   * applied — which is what we compare against emitted points.
   */
  public record IndexDocsStats(long count, long deleted) {
    public long countPlusDeleted() {
      return count + deleted;
    }
  }

  /**
   * Read live doc-count + deleted-count for the index the given alias resolves to.
   * Uses the indices stats API (total across primaries+replicas folded to primaries
   * via the alias) so docs.deleted is available. Returns {@code null} when the alias
   * can't be probed, so the caller can decide to fail-open rather than block a build
   * on a transient stats hiccup.
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
   * Decide whether a freshly-built, now-live points index is acceptably complete.
   * Pure (no I/O) so it is directly unit-testable. The "expected" number of live
   * documents is {@code emittedPoints - genuineDataFailures} (transient charges are
   * NOT subtracted: those ops were likely committed by ES even though the client
   * timed out, so they should still be present in the live count).
   *
   * <p>Because re-emitting the same id overwrites in place, the comparison is made
   * against {@code count + deleted} (the number of distinct index ops that landed),
   * not the bare live {@code count} — otherwise legitimate dedup overwrites would
   * look like loss. The build fails when the landed count is short of expected by
   * more than {@code shortfallTolerance} (a fraction, e.g. 0.001 for 0.1%).
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
