package il.org.osm.israelhiking;

import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.RELATION;
import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.WAY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.math.NumberUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.conn.ConnectTimeoutException;
import org.elasticsearch.client.ResponseException;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

public class PlanetSearchProfile implements Profile {
  private static final Logger LOGGER = Logger.getLogger(PlanetSearchProfile.class.getName());

  private PlanetilerConfig config;
  private ElasticsearchClient esClient;
  private final String pointsIndexName;
  private final String bboxIndexName;
  private final String[] supportedLanguages;
  // QRank lookup (Wikimedia pageviews by wikidata id) for the prominence signal.
  // Empty index when no QRank file is provided (local builds) — lookups return 0.
  private final QRankIndex qrankIndex;

  // Bulk indexer: thread-safe, buffers operations and flushes them in batches.
  // Planetiler emits features from multiple worker threads, so BulkIngester
  // (which handles concurrent add() internally) is the right tool rather than
  // manually batching BulkRequests with our own locking.
  private final BulkIngester<Void> bulkIngester;
  // Counters so indexing is observable instead of failing silently.
  private final LongAdder indexedCount = new LongAdder();
  // Failures are split by destination index so we can fail the build on the ones
  // that cause missing SEARCH results (points) while only warning on bbox
  // geo_shape rejects (a few degenerate OSM relation geometries ES can't parse;
  // they don't affect name search). failedCount stays the SUM so the lossless
  // invariant emitted == indexed + failed still holds.
  private final LongAdder failedPointsCount = new LongAdder();
  private final LongAdder failedBboxCount = new LongAdder();
  // Step D — indexer resilience: a SEPARATE bucket for points ops charged only
  // AFTER the bounded retry/backoff in the whole-batch (Throwable) path is
  // exhausted. These are TRANSIENT charges (client-side connection/socket
  // timeouts, 429/5xx) — likely phantom (ES may have committed them after the
  // client gave up). They are tolerated far more generously than genuine per-item
  // data failures, and the post-build reconcile gate is the real backstop. Kept
  // distinct from failedPointsCount so a real mass mapping break (per-item 4xx)
  // still trips the strict guard. transientBbox is the bbox analogue (warn-only).
  private final LongAdder transientPointsCharges = new LongAdder();
  private final LongAdder transientBboxCharges = new LongAdder();
  // Emitted/attempted docs: incremented once per bulkIngester.add(...). Lets us
  // assert the lossless invariant emitted == indexed + failed after flush() so a
  // doc that was emitted but never made it into any bulk batch is detectable.
  private final LongAdder emittedCount = new LongAdder();
  // Step D — emitted POINTS only (excludes bbox). The reconcile gate and the
  // points failure thresholds must be denominated in points, not points+bbox.
  private final LongAdder emittedPointsCount = new LongAdder();

  public static final String POINTS_LAYER_NAME = "global_points";

  private static final Map<String, MinWayIdFinder> Singles = new ConcurrentHashMap<>();
  private static final Map<String, MinWayIdFinder> NamedHighways = new ConcurrentHashMap<>();
  private static final Map<String, MinWayIdFinder> Waterways = new ConcurrentHashMap<>();

  public PlanetSearchProfile(PlanetilerConfig config, ElasticsearchClient esClient, String pointsIndexName,
      String bboxIndexName, String[] supportedLnaguages) {
    this(config, esClient, pointsIndexName, bboxIndexName, supportedLnaguages, QRankIndex.empty());
  }

  public PlanetSearchProfile(PlanetilerConfig config, ElasticsearchClient esClient, String pointsIndexName,
      String bboxIndexName, String[] supportedLnaguages, QRankIndex qrankIndex) {
    this.config = config;
    this.esClient = esClient;
    this.pointsIndexName = pointsIndexName;
    this.supportedLanguages = supportedLnaguages;
    this.bboxIndexName = bboxIndexName;
    this.qrankIndex = qrankIndex;
    this.bulkIngester = BulkIngester.of(b -> b
        .client(esClient)
        // Flush whenever any of these thresholds is hit.
        .maxOperations(5_000)
        .maxSize(5 * 1024 * 1024)
        .maxConcurrentRequests(4)
        // Listener gives us per-batch visibility instead of swallowing errors.
        // Extracted into a named class (AccountingBulkListener) so the counting
        // logic is directly unit-testable; behavior is unchanged.
        .listener(new AccountingBulkListener(esClient, indexedCount, failedPointsCount, failedBboxCount,
            transientPointsCharges, transientBboxCharges, pointsIndexName)));
  }

  /**
   * BulkListener that surfaces and counts every bulk outcome instead of
   * swallowing errors. Extracted from an anonymous inner class so the per-item
   * classification (indexed vs failed) and the whole-batch failure path are
   * unit-testable by driving {@code afterBulk} directly. The classification
   * logic is unchanged from the original anonymous listener.
   */
  static final class AccountingBulkListener implements BulkListener<Void> {
    // Step D — retry/backoff tuning for the whole-batch transient path.
    static final int MAX_RETRY_ATTEMPTS = 5;          // total resubmits (1s,2s,4s,8s,16s)
    static final long BASE_BACKOFF_MILLIS = 1_000L;   // first backoff
    static final long MAX_BACKOFF_MILLIS = 16_000L;   // cap per attempt

    /** Injectable sleep so the backoff is testable without real delays. */
    @FunctionalInterface
    interface Sleeper {
      void sleep(long millis) throws InterruptedException;
    }

    private final ElasticsearchClient esClient;
    private final LongAdder indexedCount;
    private final LongAdder failedPointsCount;
    private final LongAdder failedBboxCount;
    private final LongAdder transientPointsCharges;
    private final LongAdder transientBboxCharges;
    private final String pointsIndexName;
    private final Sleeper sleeper;

    AccountingBulkListener(ElasticsearchClient esClient, LongAdder indexedCount, LongAdder failedPointsCount,
        LongAdder failedBboxCount, LongAdder transientPointsCharges, LongAdder transientBboxCharges,
        String pointsIndexName) {
      this(esClient, indexedCount, failedPointsCount, failedBboxCount, transientPointsCharges,
          transientBboxCharges, pointsIndexName, Thread::sleep);
    }

    // Test seam: lets unit tests inject a no-op/recording Sleeper.
    AccountingBulkListener(ElasticsearchClient esClient, LongAdder indexedCount, LongAdder failedPointsCount,
        LongAdder failedBboxCount, LongAdder transientPointsCharges, LongAdder transientBboxCharges,
        String pointsIndexName, Sleeper sleeper) {
      this.esClient = esClient;
      this.indexedCount = indexedCount;
      this.failedPointsCount = failedPointsCount;
      this.failedBboxCount = failedBboxCount;
      this.transientPointsCharges = transientPointsCharges;
      this.transientBboxCharges = transientBboxCharges;
      this.pointsIndexName = pointsIndexName;
      this.sleeper = sleeper;
    }

    private boolean isPoints(String index) {
      return pointsIndexName != null && pointsIndexName.equals(index);
    }

    // A failed item destined for the points index means a missing SEARCH result
    // (build-breaking); a failed bbox item is a geometry reject (warn-only). This
    // bucket is for GENUINE per-item data failures (4xx mapping/parse).
    private void recordFailure(String index) {
      if (isPoints(index)) {
        failedPointsCount.increment();
      } else {
        failedBboxCount.increment();
      }
    }

    // The TRANSIENT bucket: whole-batch ops charged only after retries are
    // exhausted. Tolerated generously and backstopped by the reconcile gate.
    private void recordTransient(String index) {
      if (isPoints(index)) {
        transientPointsCharges.increment();
      } else {
        transientBboxCharges.increment();
      }
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request, List<Void> contexts) {
      // no-op
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, BulkResponse response) {
      if (response.errors()) {
        response.items().forEach(item -> {
          if (item.error() != null) {
            recordFailure(item.index());
            LOGGER.warning(() -> "Failed to index id=" + item.id() + " into " + item.index()
                + ": " + item.error().reason());
          } else {
            indexedCount.increment();
          }
        });
      } else {
        indexedCount.add(response.items().size());
      }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, Throwable failure) {
      // A whole-batch failure has no per-item breakdown. The 8.x BulkIngester does
      // NOT auto-resubmit on a Throwable, so we classify and (for transient errors)
      // retry the batch with exponential backoff + jitter before charging anything.
      if (isRetryable(failure)) {
        LOGGER.warning(() -> "Bulk request " + executionId + " failed transiently ("
            + describe(failure) + "); retrying with backoff.");
        resubmitWithBackoff(executionId, request.operations());
      } else {
        // Non-retryable whole-batch error (e.g. a 4xx request-level rejection): do
        // NOT retry (would loop forever) — charge as genuine data failures so a real
        // structural problem trips the strict guard.
        LOGGER.severe(() -> "Bulk request " + executionId + " failed non-retryably ("
            + describe(failure) + "); charging " + request.operations().size()
            + " op(s) as genuine data failures.");
        request.operations().forEach(op -> recordFailure(bulkOperationIndex(op)));
      }
    }

    /**
     * Resubmit a timed-out batch's operations via a fresh synchronous bulk call,
     * with exponential backoff + jitter, up to {@link #MAX_RETRY_ATTEMPTS} times.
     * After each attempt the response is reconciled per item: successes are counted
     * as indexed, per-item 4xx errors are charged as genuine data failures (never
     * retried), and only ops that hit a retryable condition (or a whole-request
     * retryable throwable) carry over to the next attempt. Whatever remains after
     * the final attempt is charged to the TRANSIENT bucket.
     */
    private void resubmitWithBackoff(long executionId, List<BulkOperation> operations) {
      List<BulkOperation> pending = new ArrayList<>(operations);
      for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS && !pending.isEmpty(); attempt++) {
        long backoff = backoffMillis(attempt);
        try {
          sleeper.sleep(backoff);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          LOGGER.warning("Retry interrupted; charging remaining " + pending.size()
              + " op(s) as transient.");
          break;
        }
        final List<BulkOperation> batch = pending;
        try {
          BulkResponse response = esClient.bulk(b -> b.operations(batch));
          pending = reconcileRetryResponse(batch, response);
          if (pending.isEmpty()) {
            final int n = batch.size();
            final int a = attempt;
            LOGGER.info(() -> "Bulk request " + executionId + " recovered on retry attempt "
                + a + " (" + n + " op(s) re-applied).");
            return;
          }
        } catch (Exception e) {
          if (!isRetryable(e)) {
            // The whole resubmit hit a non-retryable error — charge all pending as
            // genuine data failures and stop (retrying can't help).
            LOGGER.severe(() -> "Retry of bulk request " + executionId
                + " hit a non-retryable error (" + describe(e) + "); charging "
                + batch.size() + " op(s) as genuine data failures.");
            batch.forEach(op -> recordFailure(bulkOperationIndex(op)));
            return;
          }
          final int a = attempt;
          LOGGER.warning(() -> "Retry attempt " + a + " for bulk request " + executionId
              + " failed transiently (" + describe(e) + ").");
          // keep `pending` as-is for the next attempt
        }
      }
      // Retries exhausted (or interrupted): charge whatever is still pending to the
      // TRANSIENT bucket — likely phantom (ES may have committed despite the timeout),
      // and the reconcile gate is the real backstop.
      if (!pending.isEmpty()) {
        final int n = pending.size();
        LOGGER.warning(() -> "Bulk request " + executionId + " exhausted retries; charging "
            + n + " op(s) to the transient bucket.");
        pending.forEach(op -> recordTransient(bulkOperationIndex(op)));
      }
    }

    /**
     * Classify a retry-attempt's response: count successes, charge per-item 4xx as
     * genuine data failures, and return the ops that should be retried again
     * (per-item retryable status: 429 or 5xx). An op with no matching response item
     * is conservatively kept for retry.
     */
    private List<BulkOperation> reconcileRetryResponse(List<BulkOperation> batch, BulkResponse response) {
      // Walk per OP (not per response item): a response shorter than the batch must NOT silently
      // drop the surplus ops. Whether response.errors() is set or not, an op with no matching
      // response item is kept for retry so it can never vanish from the emitted/indexed/failed
      // accounting (the lossless invariant the reconcile gate relies on).
      List<BulkOperation> retryAgain = new ArrayList<>();
      List<BulkResponseItem> items = response.items();
      for (int i = 0; i < batch.size(); i++) {
        BulkResponseItem item = i < items.size() ? items.get(i) : null;
        if (item == null) {
          retryAgain.add(batch.get(i)); // no response for this op — retry it, don't drop it
        } else if (item.error() == null) {
          indexedCount.increment();
        } else if (isRetryableStatus(item.status())) {
          retryAgain.add(batch.get(i));
        } else {
          // Genuine per-item failure (e.g. 400 mapping/parse) — must NOT be retried.
          recordFailure(item.index());
          LOGGER.warning(() -> "Failed to index id=" + item.id() + " into " + item.index()
              + ": " + item.error().reason());
        }
      }
      return retryAgain;
    }

    // Exponential backoff with full jitter, capped. attempt is 1-based.
    static long backoffMillis(int attempt) {
      long exp = BASE_BACKOFF_MILLIS << (attempt - 1); // 1s,2s,4s,8s,16s,...
      long capped = Math.min(exp, MAX_BACKOFF_MILLIS);
      // Full jitter in [capped/2, capped] keeps it bounded but de-synchronised
      // across the 4 concurrent bulk workers.
      long half = capped / 2;
      return half + ThreadLocalRandom.current().nextLong(half + 1);
    }

    /**
     * Retryable = transient transport/availability problems that a resubmit can
     * fix: connection/socket timeouts, connection refused, generic IO, and HTTP
     * 429/502/503/504. Non-retryable = 4xx request/data errors (esp. 400 mapping
     * /parse) which would loop forever.
     */
    static boolean isRetryable(Throwable t) {
      for (Throwable c = t; c != null; c = c.getCause()) {
        if (c instanceof SocketTimeoutException
            || c instanceof ConnectTimeoutException
            || c instanceof ConnectException) {
          return true;
        }
        if (c instanceof ResponseException re) {
          return isRetryableStatus(re.getResponse().getStatusLine().getStatusCode());
        }
        if (c instanceof ElasticsearchException ese) {
          return isRetryableStatus(ese.status());
        }
        if (c instanceof IOException) {
          // Generic IO (connection reset, broken pipe, premature EOF) — transient.
          return true;
        }
        if (c.getCause() == c) {
          break; // guard against self-referential cause chains
        }
      }
      return false;
    }

    static boolean isRetryableStatus(int status) {
      return status == 429 || status == 502 || status == 503 || status == 504;
    }

    private static String describe(Throwable t) {
      String msg = t.getMessage();
      return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }

    // Best-effort extraction of the destination index from a bulk operation
    // (index/create/update/delete variants). Null when it can't be determined,
    // which the failure/transient recorders treat as non-points (bbox) — the
    // conservative choice for the warn-only bucket.
    private static String bulkOperationIndex(BulkOperation op) {
      if (op.isIndex()) {
        return op.index().index();
      } else if (op.isCreate()) {
        return op.create().index();
      } else if (op.isUpdate()) {
        return op.update().index();
      } else if (op.isDelete()) {
        return op.delete().index();
      }
      return null;
    }
  }

  /*
   * The processing happens in 3 steps:
   * 1. On the first pass through the input file, store relevant information from
   * applicable OSM route relations and ways with mtb:name tag.
   * 2. On the second pass, emit points for relation and mtb:name ways. Emit a
   * point by merging all the ways and using the first point of the merged
   * linestring.
   * 
   * Step 1)
   *
   * Planetiler processes the .osm.pbf input file in two passes. The first pass
   * stores node locations, and invokes
   * preprocessOsmRelation for reach relation and stores information the profile
   * needs during the second pass when we
   * emit map feature for ways contained in that relation.
   * 
   * Step 2)
   *
   * On the second pass through the input .osm.pbf file, for each way in a
   * relation that we stored data about, emit a
   * point with attributes derived from the relation as well as for ways with
   * mtb:name tag.
   */

  static private final void CoalesceIntoMap(Map<String, String> map, String language, String... strings) {
    var value = Arrays.stream(strings)
        .filter(Objects::nonNull)
        .filter(s -> !s.isEmpty())
        .findFirst()
        .orElse(null);
    if (value != null) {
      map.put(language, value);
    }
  }

  /**
   * Language-neutral OSM variant-name tag bases (no {@code :<lang>} suffix). These also have
   * per-language forms ({@code alt_name:he}, {@code official_name:en}, ...). {@code old_name} is
   * DELIBERATELY EXCLUDED — it is a documented Nominatim noise source (a stale former name can
   * overtake the real one). See ADR-0011.
   */
  static private final String[] ALT_NAME_TAG_BASES = {
      "alt_name", "official_name", "short_name", "loc_name", "int_name"
  };

  /**
   * Populate {@code pointDocument.alt_names} (ADR-0011 item 1) from the OSM variant-name tags,
   * keyed by language exactly like {@code name}: the language-suffixed tags
   * ({@code alt_name:<lang>}, ...) go under {@code <lang>}, and the bare tags ({@code alt_name},
   * ...) go under {@code "default"}.
   *
   * <p>CRITICAL difference from {@link #CoalesceIntoMap}: alt-name tags are frequently MULTI-VALUED
   * with {@code ;} (e.g. {@code alt_name=A;B;C}). CoalesceIntoMap keeps only the FIRST tag and never
   * splits, so it cannot be reused here. We split every value on {@code ;}, trim, drop empties, and
   * de-duplicate (preserving order) into a {@code List<String>} so each variant is a SEPARATE value
   * (ES indexes a string array as independent tokens — no {@code ;}-joining, which would fold the
   * variants back into one analyzed string). The map (and each language entry) is created lazily,
   * so a feature with no variant tags leaves {@code alt_names} null (omitted from JSON).
   */
  private void addAltNames(PointDocument pointDocument, WithTags feature) {
    var altNames = buildAltNames(supportedLanguages, feature::getString);
    if (altNames != null) {
      pointDocument.alt_names = altNames;
    }
  }

  /**
   * PURE (no I/O, no {@code WithTags}) builder for the {@code alt_names} map, so the {@code ;}-split,
   * trim, de-dup and old_name-exclusion logic is directly unit-testable (mirrors the
   * {@code flooredProminence} extraction pattern). Returns {@code null} when no variant tags are
   * present (so {@code addAltNames} leaves {@code alt_names} null and it is omitted from JSON).
   *
   * @param supportedLanguages the per-language keys to look up suffixed tags for
   * @param tagLookup          maps an OSM tag key to its raw value (null if absent)
   */
  static Map<String, List<String>> buildAltNames(String[] supportedLanguages,
      java.util.function.Function<String, String> tagLookup) {
    Map<String, List<String>> altNames = null;
    for (String language : supportedLanguages) {
      var suffixedTags = Arrays.stream(ALT_NAME_TAG_BASES)
          .map(base -> base + ":" + language)
          .toArray(String[]::new);
      var collected = collectVariants(tagLookup, suffixedTags);
      if (collected != null) {
        if (altNames == null) {
          altNames = new HashMap<String, List<String>>();
        }
        altNames.put(language, collected);
      }
    }
    // Bare (language-neutral) tags -> "default".
    var collectedDefault = collectVariants(tagLookup, ALT_NAME_TAG_BASES);
    if (collectedDefault != null) {
      if (altNames == null) {
        altNames = new HashMap<String, List<String>>();
      }
      altNames.put("default", collectedDefault);
    }
    return altNames;
  }

  /**
   * Read each of {@code tags}, split every value on {@code ;}, trim, drop empties, de-duplicate
   * (insertion order preserved). Returns {@code null} if nothing survives (so the caller can skip
   * the key entirely). {@code old_name} is never in {@code tags}.
   */
  private static List<String> collectVariants(java.util.function.Function<String, String> tagLookup,
      String[] tags) {
    var variants = new java.util.LinkedHashSet<String>();
    for (String tag : tags) {
      var raw = tagLookup.apply(tag);
      if (raw == null || raw.isEmpty()) {
        continue;
      }
      for (String part : raw.split(";")) {
        var trimmed = part.trim();
        if (!trimmed.isEmpty()) {
          variants.add(trimmed);
        }
      }
    }
    return variants.isEmpty() ? null : new ArrayList<>(variants);
  }

  private void convertTagsToDocument(PointDocument pointDocument, WithTags feature) {
    for (String language : supportedLanguages) {
      CoalesceIntoMap(pointDocument.name, language, feature.getString("name:" + language));
      CoalesceIntoMap(pointDocument.description, language, feature.getString("description:" + language));
    }
    if (feature.hasTag("name")) {
      CoalesceIntoMap(pointDocument.name, "default", feature.getString("name"));
    }
    if (feature.hasTag("description")) {
      CoalesceIntoMap(pointDocument.description, "default", feature.getString("description"));
    }
    // ADR-0011 item 1 — variant names into the SEPARATE alt_names field (after the name loop).
    // convertTagsToDocument is the single chokepoint that also serves relations (the IBT case),
    // so this one call covers the relation path too.
    addAltNames(pointDocument, feature);
    setDifficulty(pointDocument, feature);
    pointDocument.wikidata = feature.getString("wikidata");
    pointDocument.image = feature.getString("image");
    pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
    pointDocument.website = feature.getString("website");
    setProminence(pointDocument, feature);
    setPopulation(pointDocument, feature);
    setFeatureClass(pointDocument, feature);
  }

  /**
   * Derive a coarse FEATURE TYPE ("peak", "lake", "waterfall", "city", "spring", ...) from the
   * primary OSM type tags and store it on {@code pointDocument.feature_class}. This is the kind-of-
   * thing signal the engine needs to rank "Mount Shasta" as a SUMMIT, not a homonym hotel — distinct
   * from poiCategory ("Wikipedia"/"Other"), which never identifies the feature type.
   *
   * <p>Reads the same tags {@link #setProminence} already consults (no new tag access). Most-specific
   * tag wins; leaves the field null when nothing recognised is present (omitted from JSON, so the
   * ~22.8M docs without a known type carry no extra bytes). Normalises OSM values into a small,
   * stable vocabulary so the query side can match on a known set rather than raw OSM strings.
   */
  void setFeatureClass(PointDocument pointDocument, WithTags feature) {
    pointDocument.feature_class = classifyFeatureClass(feature::getString);
  }

  /**
   * PURE (no {@code WithTags}) core of {@link #setFeatureClass}, so the OSM-tag -> feature_class
   * switch is directly unit-testable from a literal tag map (mirrors {@code buildAltNames} /
   * {@code flooredProminence}). Returns null when nothing recognised is present.
   */
  static String classifyFeatureClass(java.util.function.Function<String, String> tagLookup) {
    String natural = tagLookup.apply("natural");
    String waterway = tagLookup.apply("waterway");
    String place = tagLookup.apply("place");
    String historic = tagLookup.apply("historic");
    String tourism = tagLookup.apply("tourism");
    // Built/POI keys (ADR-0019): consulted only AFTER the outdoor keys above, so an object
    // carrying both an outdoor tag and an incidental built tag (e.g. tourism=viewpoint that is
    // also building=yes) keeps its outdoor class. Unnamed objects never reach this code — every
    // emit path gates on name (processOtherSourceFeature, addNonIconFeaturesToElasricseach), so
    // building=yes noise is already filtered upstream and we classify only named features.
    String leisure = tagLookup.apply("leisure");
    String amenity = tagLookup.apply("amenity");
    String shop = tagLookup.apply("shop");
    String office = tagLookup.apply("office");
    String craft = tagLookup.apply("craft");
    String healthcare = tagLookup.apply("healthcare");
    String manMade = tagLookup.apply("man_made");
    String building = tagLookup.apply("building");

    String fc = null;
    if (natural != null) {
      switch (natural) {
        case "peak":                  fc = "peak"; break;
        case "volcano":               fc = "peak"; break;
        case "hill":                  fc = "hill"; break;
        case "ridge":                 fc = "ridge"; break;
        case "saddle": case "gap":    fc = "saddle"; break;
        case "cliff":                 fc = "cliff"; break;
        case "rock": case "stone":    fc = "rock"; break;
        case "water":                 fc = "lake"; break;   // refined below by water=*
        case "spring": case "hot_spring": fc = "spring"; break;
        case "glacier":               fc = "glacier"; break;
        case "bay":                   fc = "bay"; break;
        case "cape":                  fc = "cape"; break;
        case "beach":                 fc = "beach"; break;
        case "wood": case "forest":   fc = "forest"; break;
        case "valley":                fc = "valley"; break;
        // landforms scoring already groups (TERRAIN_RELIEF) but the indexer never emitted:
        case "canyon": case "gorge":  fc = "canyon"; break;
        case "mesa": case "plateau":  fc = "plateau"; break;
        case "arch":                  fc = "arch"; break;
        case "cave_entrance":         fc = "cave"; break;
        case "wetland":               fc = "wetland"; break;
        case "arete":                 fc = "ridge"; break;    // fold: ridge kin
        case "crater":                fc = "peak"; break;     // fold: volcanic-summit kin
        case "mountain_range":        fc = "peak"; break;     // fold: high-ground kin
        default:                      fc = "natural"; break;
      }
      // refine natural=water by its sub-type (lake vs reservoir vs pond)
      String water = tagLookup.apply("water");
      if ("water".equals(natural) && water != null) {
        switch (water) {
          case "lake":      fc = "lake"; break;
          case "reservoir": fc = "reservoir"; break;
          case "pond":      fc = "pond"; break;
          default:          fc = "lake"; break;
        }
      }
    } else if (waterway != null) {
      switch (waterway) {
        case "waterfall":             fc = "waterfall"; break;
        case "river":                 fc = "river"; break;
        case "stream":                fc = "stream"; break;
        case "canal":                 fc = "canal"; break;
        case "rapids":                fc = "rapids"; break;
        default:                      fc = "waterway"; break;
      }
    } else if (place != null) {
      switch (place) {
        case "city": case "town": case "village": case "hamlet":
        case "suburb": case "neighbourhood":
                                      fc = place; break;
        case "island": case "islet": fc = "island"; break;
        case "locality":             fc = "locality"; break;
        default:                      fc = "place"; break;
      }
    } else if (historic != null) {
      fc = "historic";
    } else if (tourism != null) {
      switch (tourism) {
        case "viewpoint":             fc = "viewpoint"; break;
        case "camp_site":             fc = "campsite"; break;
        case "attraction":            fc = "attraction"; break;
        // lodging (built): folded into one "lodging" class
        case "hotel": case "motel": case "hostel": case "guest_house":
        case "apartment": case "chalet": case "alpine_hut": case "wilderness_hut":
                                      fc = "lodging"; break;
        case "museum": case "gallery": fc = "museum"; break;
        case "information":           fc = "tourism"; break;
        default:                      fc = "tourism"; break;
      }
    } else if (leisure != null) {
      // Recreation: park/nature_reserve lean OUTDOOR (semi-outdoor group), sports the rest.
      switch (leisure) {
        case "park": case "garden": case "nature_reserve":
        case "playground": case "dog_park": case "common":
                                      fc = "park"; break;
        case "sports_centre": case "stadium": case "pitch": case "track":
        case "fitness_centre": case "swimming_pool": case "golf_course":
        case "ice_rink": case "horse_riding":
                                      fc = "sports"; break;
        default:                      fc = "leisure"; break;
      }
    } else if (amenity != null) {
      switch (amenity) {
        case "restaurant": case "cafe": case "fast_food": case "bar":
        case "pub": case "food_court": case "biergarten": case "ice_cream":
                                      fc = "food"; break;
        case "place_of_worship": case "monastery":
                                      fc = "religious"; break;
        case "school": case "university": case "college": case "kindergarten":
        case "library":
                                      fc = "education"; break;
        case "hospital": case "clinic": case "pharmacy": case "doctors":
        case "dentist": case "veterinary":
                                      fc = "medical"; break;
        case "townhall": case "courthouse": case "police": case "fire_station":
        case "post_office": case "embassy": case "prison":
                                      fc = "government"; break;
        case "fuel": case "charging_station":
                                      fc = "fuel"; break;
        case "parking": case "parking_space": case "bicycle_parking":
        case "motorcycle_parking": case "taxi":
                                      fc = "parking"; break;
        case "bus_station": case "ferry_terminal":
                                      fc = "transit"; break;
        case "theatre": case "cinema": case "arts_centre":
                                      fc = "museum"; break;
        default:                      fc = "amenity"; break;
      }
    } else if (shop != null) {
      fc = "shop";
    } else if (office != null) {
      fc = "office";
    } else if (craft != null) {
      fc = "office";
    } else if (healthcare != null) {
      fc = "medical";
    } else if (manMade != null) {
      switch (manMade) {
        case "tower": case "lighthouse": case "bridge": case "obelisk":
        case "water_tower": case "windmill": case "watermill": case "pier":
                                      fc = "structure"; break;
        default:                      fc = "man_made"; break;
      }
    } else if (building != null && !"no".equals(building) && !"none".equals(building)
               && !"No".equals(building)) {
      // Named building with no more-specific type tag above. building=yes is the dominant
      // (~80%) value; a named one is a real search target (a named hall/landmark), so it gets
      // the generic "building" class rather than null.
      switch (building) {
        case "church": case "chapel": case "cathedral": case "mosque":
        case "synagogue": case "temple": case "shrine":
                                      fc = "religious"; break;
        case "hotel":                 fc = "lodging"; break;
        case "hospital":              fc = "medical"; break;
        case "school": case "university": case "college":
                                      fc = "education"; break;
        case "train_station":         fc = "transit"; break;
        default:                      fc = "building"; break;
      }
    }
    return fc;
  }

  /**
   * Compute the composite prominence score from OSM tags + QRank and store it (plus raw components,
   * for re-tuning without a reindex) on the document. Reads tags directly from the feature rather
   * than poiCategory, because poiCategory is assigned later in some emit paths.
   */
  private void setProminence(PointDocument pointDocument, WithTags feature) {
    long qrankRaw = qrankIndex.getByWikidata(pointDocument.wikidata);

    // Elevation feeds the peak prominence prior, read from the OSM ele tag (NaN when absent).
    double ele = parseFirstNumber(feature.getString("ele"));

    boolean hasImage = pointDocument.image != null || pointDocument.wikimedia_commons != null;
    boolean hasWebsite = pointDocument.website != null;
    boolean hasWikidata = pointDocument.wikidata != null;

    ProminenceCalculator.Result r = ProminenceCalculator.compute(
        feature.getString("natural"),
        feature.getString("place"),
        feature.getString("boundary"),
        feature.getString("tourism"),
        feature.getString("historic"),
        feature.getString("waterway"),
        ele, hasImage, hasWebsite, hasWikidata, qrankRaw);

    pointDocument.prominence = r.prominence;
    pointDocument.prom_base = r.base;
    pointDocument.prom_qrank_norm = r.qrankNorm;
    pointDocument.prom_meta = r.meta;
    pointDocument.ele_norm = r.eleNorm;
    pointDocument.qrank_raw = r.qrankRaw > 0 ? r.qrankRaw : null;

    setEnrichmentSignals(pointDocument, feature);
  }

  /**
   * ADR-0014 — additive ranking signals beyond the prominence composite:
   * <ul>
   *   <li>{@code area_norm}: log-normalized polygon area, only when the feature carries polygon
   *       geometry (a {@link SourceFeature} that {@code canBePolygon()}). Left null for points/lines
   *       and for relations routed through {@code convertTagsToDocument} as {@link WithTags}.</li>
   *   <li>{@code intermittent}: the OSM {@code intermittent=yes} flag (seasonal/dry water).</li>
   * </ul>
   */
  private void setEnrichmentSignals(PointDocument pointDocument, WithTags feature) {
    // area_norm — geometry-dependent, so only when we actually hold a polygon-capable SourceFeature.
    if (feature instanceof SourceFeature sf && sf.canBePolygon()) {
      try {
        pointDocument.area_norm = normalizeArea(sf.areaMeters());
      } catch (Exception e) {
        // Bad polygon geometry — leave area_norm null, never fail the build. Logged at FINE
        // rather than WARNING because this fires in the high-volume point-enrichment path and a
        // few unbuildable polygons among ~22.8M features is an expected data condition; FINE keeps
        // it diagnosable (enable il.org.osm.israelhiking at FINE) without flooding the console.
        LOGGER.fine(() -> "area_norm skipped for " + sourceFeatureToDocumentId(sf)
            + " (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ")");
      }
    }

    // intermittent — only set the flag when present (NON_NULL keeps it off the other ~22.8M docs).
    if (feature.hasTag("intermittent", "yes")) {
      pointDocument.intermittent = Boolean.TRUE;
    }
  }

  /** {@code log1p(areaM)/log1p(MAX_AREA_M2)} clamped to [0,1]; MAX_AREA_M2 = 1e11 (~100k km²). */
  private static float normalizeArea(double areaM) {
    if (Double.isNaN(areaM) || areaM <= 0) {
      return 0f;
    }
    double norm = Math.log1p(areaM) / Math.log1p(1e11);
    return (float) Math.max(0.0, Math.min(1.0, norm));
  }

  /** Population is a place/admin-layer signal only — set it for settlements, leave POIs null. */
  private void setPopulation(PointDocument pointDocument, WithTags feature) {
    String place = feature.getString("place");
    if (place == null) {
      return;
    }
    double parsed = parseFirstNumber(feature.getString("population"));
    if (!Double.isNaN(parsed) && parsed > 0) {
      pointDocument.population = (int) Math.min(parsed, Integer.MAX_VALUE);
      return;
    }
    // Ladder fallback when the population tag is missing (covers the ~80% of villages/hamlets
    // that have no number). A real value always overrides this.
    switch (place) {
      case "city":    pointDocument.population = 1_000_000; break;
      case "town":    pointDocument.population = 50_000; break;
      case "village": pointDocument.population = 2_000; break;
      case "hamlet":  pointDocument.population = 200; break;
      default:        pointDocument.population = 20; break;
    }
  }

  /**
   * Parse the first number out of a free-text OSM value like "4302", "14,115 ft", "1 000", "yes".
   * Returns Double.NaN when there is no usable number. Never throws.
   */
  static double parseFirstNumber(String raw) {
    if (raw == null) {
      return Double.NaN;
    }
    StringBuilder sb = new StringBuilder();
    boolean seenDigit = false;
    boolean seenDot = false;
    for (int i = 0; i < raw.length(); i++) {
      char c = raw.charAt(i);
      if (c >= '0' && c <= '9') {
        sb.append(c);
        seenDigit = true;
      } else if (c == '.' && seenDigit && !seenDot) {
        sb.append(c);
        seenDot = true;
      } else if ((c == ',' || c == ' ' || c == '\'') && seenDigit) {
        // thousands separator within a number — skip it
      } else if (seenDigit) {
        break; // number ended (e.g. " ft", "-")
      }
    }
    if (!seenDigit) {
      return Double.NaN;
    }
    try {
      return Double.parseDouble(sb.toString());
    } catch (NumberFormatException e) {
      return Double.NaN;
    }
  }

  private void setDifficulty(PointDocument pointDocument, WithTags feature) {
    if (feature.hasTag("sac_scale")) {
      switch (feature.getString("sac_scale")) {
        case "none":
          pointDocument.poiDifficulty = "Easy";
          break;
        case "T1":
          pointDocument.poiDifficulty = "Moderate";
          break;
        case "T2":
          pointDocument.poiDifficulty = "Hard";
          break;
        case "T3":
        case "T4":
        case "T5":
        case "T6":
          pointDocument.poiDifficulty = "Very Hard";
          break;
      }
    } else if (feature.hasTag("mtb:scale")) {
      switch (feature.getString("mtb:scale")) {
        case "0":
          pointDocument.poiDifficulty = "Easy";
          break;
        case "1":
          pointDocument.poiDifficulty = "Moderate";
          break;
        case "2":
          pointDocument.poiDifficulty = "Hard";
          break;
        case "3":
        case "4":
        case "5":
        case "6":
          pointDocument.poiDifficulty = "Very Hard";
          break;
      }
    } else if (feature.hasTag("tracktype")) {
      switch (feature.getString("tracktype")) {
        case "grade1":
        case "grade2":
          pointDocument.poiDifficulty = "Easy";
          break;
        case "grade3":
          pointDocument.poiDifficulty = "Moderate";
          break;
        case "grade4":
          pointDocument.poiDifficulty = "Hard";
          break;
        case "grade5":
          pointDocument.poiDifficulty = "Very Hard";
          break;
      }
    }
  }

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    // If this is a "route" relation ...
    if (relation.hasTag("state", "proposed")) {
      return null;
    }
    var pointDocument = new PointDocument();
    setIconColorCategory(pointDocument, relation);

    if (!"icon-river".equals(pointDocument.poiIcon) &&
        !"Bicycle".equals(pointDocument.poiCategory) &&
        !"Hiking".equals(pointDocument.poiCategory) &&
        !"4x4".equals(pointDocument.poiCategory)) {
      return null;
    }
    // then store a RouteRelationInfo instance with tags we'll need later
    var waysMemberIds = relation.members()
        .stream()
        .filter(member -> member.type() == WAY)
        .mapToLong(OsmElement.Relation.Member::ref)
        .boxed()
        .collect(Collectors.toList());

    var relationMemberIds = relation.members()
        .stream()
        .filter(member -> member.type() == RELATION)
        .mapToLong(OsmElement.Relation.Member::ref)
        .boxed()
        .collect(Collectors.toList());

    if (waysMemberIds.isEmpty() && relationMemberIds.isEmpty()) {
      return null;
    }
    var info = new RelationInfo(relation.id());

    convertTagsToDocument(pointDocument, relation);

    pointDocument.poiSource = "OSM";
    info.pointDocument = pointDocument;
    if (waysMemberIds.size() > 0) {
      info.firstMemberId = waysMemberIds.getFirst();
      info.secondMemberId = waysMemberIds.size() > 1 ? waysMemberIds.get(1) : -1;
    } else if (relationMemberIds.size() > 0) {
      info.firstMemberId = relationMemberIds.getFirst();
      info.secondMemberId = relationMemberIds.size() > 1 ? relationMemberIds.get(1) : -1;
    }

    info.waysMemberIds = Collections.synchronizedList(waysMemberIds);
    info.RelationMemberIds = Collections.synchronizedList(relationMemberIds);
    info.isSuperRelation = info.RelationMemberIds.size() > 0;
    return List.of(info);
  }

  @Override
  public void preprocessOsmWay(OsmElement.Way way) {
    if (way.hasTag("mtb:name")) {
      String mtbName = way.getString("mtb:name");
      synchronized (mtbName.intern()) {
        if (!Singles.containsKey(mtbName)) {
          var finder = new MinWayIdFinder();
          finder.ids.add(way.id());
          Singles.put(mtbName, finder);
        } else {
          Singles.get(mtbName).ids.add((way.id()));
        }
        return;
      }
    }
    if (way.hasTag("waterway") && way.hasTag("name")) {
      String waterwayName = way.getString("name");
      synchronized (waterwayName.intern()) {
        if (!Waterways.containsKey(waterwayName)) {
          var finder = new MinWayIdFinder();
          finder.ids.add((way.id()));
          Waterways.put(waterwayName, finder);
        } else {
          Waterways.get(waterwayName).ids.add((way.id()));
        }
      }
      return;
    }

    if (way.hasTag("highway", "track", "path", "footway", "cycleway") && way.hasTag("name")) {
      String highwayName = way.getString("name");
      synchronized (highwayName.intern()) {
        if (!NamedHighways.containsKey(highwayName)) {
          var finder = new MinWayIdFinder();
          finder.ids.add((way.id()));
          NamedHighways.put(highwayName, finder);
        } else {
          NamedHighways.get(highwayName).ids.add((way.id()));
        }
      }
      return;
    }
  }

  @Override
  public void processFeature(SourceFeature feature, FeatureCollector features) {
    try {
      if (feature.getSource() == "external") {
        processExternalFeautre(feature, features);
        return;
      }
      if (isBBoxFeature(feature, supportedLanguages)) {
        insertBboxToElasticsearch(feature, supportedLanguages);
      }
      processOsmRelationFeature(feature, features);
      if (processMtbNameFeature(feature, features))
        return;
      if (processWaterwayFeature(feature, features))
        return;
      if (processHighwayFeautre(feature, features))
        return;
      if (processOtherSourceFeature(feature, features))
        return;
      addNonIconFeaturesToElasricseach(feature);
    } catch (GeometryException e) {
      // ignore bad geometries
    }
  }

  private void processExternalFeautre(SourceFeature feature, FeatureCollector features) throws GeometryException {
    var pointDocument = new PointDocument();
    pointDocument.poiIcon = feature.getString("poiIcon");
    pointDocument.poiIconColor = feature.getString("poiIconColor");
    pointDocument.poiCategory = feature.getString("poiCategory");
    pointDocument.poiSource = feature.getString("poiSource");
    pointDocument.poiDifficulty = feature.getString("poiDifficulty");
    pointDocument.poiLength = NumberUtils.toDouble(feature.getString("poiLength"), 0.0);
    convertTagsToDocument(pointDocument, feature);
    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var docId = pointDocument.poiSource + "_" + feature.getString("identifier");
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

    insertPointToElasticsearch(pointDocument, docId);

    var tileFeature = features.geometry("external", point)
        .setAttr("poiId", docId)
        .setAttr("identifier", feature.getString("identifier"))
        .setAttr("poiUserId", feature.getString("poiUserId"))
        .setId(feature.id());
    setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
  }

  private void processOsmRelationFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    // get all the RouteRelationInfo instances we returned from
    // preprocessOsmRelation that this way belongs to, including super relations.
    for (var routeInfo : feature.relationInfo(RelationInfo.class, true)) {
      RelationInfo relation = routeInfo.relation();
      synchronized (relation) {
        if (relation.waysMemberIds.remove(feature.id())) {
          relation.length += feature.lengthMeters();
        }
        if (relation.firstMemberId == feature.id()) {
          relation.firstMemberFeature = feature;
        }
        if (relation.secondMemberId == feature.id()) {
          relation.secondMemberFeature = feature;
        }
      }
    }

    handleSuperRelationMembersUpdate(feature);

    for (var routeInfo : feature.relationInfo(RelationInfo.class, true)) {
      RelationInfo relation = routeInfo.relation();
      if (!relation.waysMemberIds.isEmpty() || !relation.RelationMemberIds.isEmpty()) {
        continue;
      }

      if (relation.pointDocument.name.isEmpty()) {
        continue;
      }
      // All relation members were reached. Add a POI element for line relation
      var point = getFirstPointOfLineRelation(relation.firstMemberFeature, relation.secondMemberFeature);
      var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
      relation.pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };
      relation.pointDocument.poiLength = relation.length;
      insertPointToElasticsearch(relation.pointDocument, "OSM_relation_" + relation.id());

      var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
          .setId(relation.vectorTileFeatureId(config.featureSourceIdMultiplier()));
      setFeaturePropertiesFromPointDocument(tileFeature, relation.pointDocument);
    }
  }

  private boolean processMtbNameFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!feature.hasTag("mtb:name")) {
      return false;
    }
    String mtbName = feature.getString("mtb:name");
    if (!Singles.containsKey(mtbName)) {
      return false;
    }
    var single = Singles.get(mtbName);
    synchronized (single) {
      single.features.add(feature);
      single.ids.remove(feature.id());

      if (!single.ids.isEmpty()) {
        return true;
      }

      for (var mergedFeature : single.getMergedFeatures()) {
        var minIdFeature = mergedFeature.representingFeature;

        var pointDocument = new PointDocument();
        convertTagsToDocument(pointDocument, feature);
        for (String language : supportedLanguages) {
          CoalesceIntoMap(pointDocument.name, language, minIdFeature.getString("mtb:name:" + language));
        }
        if (minIdFeature.hasTag("mtb:name")) {
          CoalesceIntoMap(pointDocument.name, "default", minIdFeature.getString("mtb:name"));
        }
        pointDocument.poiCategory = "Bicycle";
        pointDocument.poiIcon = "icon-bike";
        pointDocument.poiIconColor = "gray";
        pointDocument.poiSource = "OSM";
        pointDocument.poiLength = mergedFeature.length;

        var firstLine = mergedFeature.geometry;
        var point = GeoUtils.point(((Geometry) firstLine).getCoordinate());
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

        insertPointToElasticsearch(pointDocument, "OSM_way_" + mergedFeature.minId);
        // This was the last way with the same mtb:name, so we can merge the lines and
        // add the feature
        // Add a POI element for a SingleTrack
        var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
            // Override the feature id with the minimal id of the group
            .setId(minIdFeature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
        setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
      }
    }
    return true;
  }

  private boolean processWaterwayFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!feature.hasTag("waterway")) {
      return false;
    }
    if (!feature.hasTag("name")) {
      return false;
    }
    String name = feature.getString("name");
    if (!Waterways.containsKey(name)) {
      return false;
    }
    for (var routeInfo : feature.relationInfo(RelationInfo.class)) {
      RelationInfo relation = routeInfo.relation();
      if (relation.pointDocument.poiIcon == "icon-river") {
        // In case this waterway is part of a relation, we already processed it
        return true;
      }
    }

    var waterway = Waterways.get(name);
    synchronized (waterway) {

      waterway.features.add(feature);
      waterway.ids.remove(feature.id());
      if (!waterway.ids.isEmpty()) {
        return true;
      }
      for (var mergedFeature : waterway.getMergedFeatures()) {
        var minIdFeature = mergedFeature.representingFeature;

        var pointDocument = new PointDocument();
        convertTagsToDocument(pointDocument, minIdFeature);
        pointDocument.poiCategory = "Water";
        pointDocument.poiIcon = "icon-river";
        pointDocument.poiIconColor = "#1e80e3";
        pointDocument.poiSource = "OSM";
        pointDocument.poiLength = mergedFeature.length;

        var firstLine = mergedFeature.geometry;
        var point = GeoUtils.point(((Geometry) firstLine).getCoordinate());
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

        insertPointToElasticsearch(pointDocument, "OSM_way_" + mergedFeature.minId);
        if (!isInterestingPoint(pointDocument)) {
          // Skip adding features without any description or image to tiles
          continue;
        }

        var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
            // Override the feature id with the minimal id of the group
            .setId(minIdFeature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
        setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
      }
      return true;
    }
  }

  private boolean processHighwayFeautre(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!feature.hasTag("highway")) {
      return false;
    }
    if (!feature.hasTag("name")) {
      // Highways without a name should not be included in the search or POI layer.
      return true;
    }
    if (feature.isPoint()) {
      // We don't want to process highway nodes (bus stops, etc.) here.
      return false;
    }
    if (!feature.hasTag("highway", "track", "path", "footway", "cycleway")) {
      return true;
    }

    String name = feature.getString("name");
    if (!NamedHighways.containsKey(name)) {
      return true;
    }

    var highway = NamedHighways.get(name);
    synchronized (highway) {

      highway.features.add(feature);
      highway.ids.remove(feature.id());

      if (!highway.ids.isEmpty()) {
        return true;
      }

      for (var mergedFeature : highway.getMergedFeatures()) {
        var minIdFeature = mergedFeature.representingFeature;
        var pointDocument = new PointDocument();
        setIconColorCategory(pointDocument, minIdFeature);
        convertTagsToDocument(pointDocument, minIdFeature);
        pointDocument.poiSource = "OSM";
        pointDocument.poiLength = mergedFeature.length;

        var point = GeoUtils.point((mergedFeature.geometry.getCoordinate()));
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };
        insertPointToElasticsearch(pointDocument, "OSM_way_" + mergedFeature.minId);
      }

      return true;
    }
  }

  private boolean processOtherSourceFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!feature.hasTag("name") &&
        !feature.hasTag("wikidata") &&
        !feature.hasTag("image") &&
        !feature.hasTag("description") &&
        !feature.hasTag("ref:IL:inature")) {
      return false;
    }

    var tileId = feature.vectorTileFeatureId(config.featureSourceIdMultiplier());
    var docId = sourceFeatureToDocumentId(feature);
    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());

    var pointDocument = new PointDocument();
    convertTagsToDocument(pointDocument, feature);
    pointDocument.poiSource = "OSM";
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

    setIconColorCategory(pointDocument, feature);

    if (pointDocument.poiIcon == "icon-search") {
      return false;
    }

    if (feature.getString("place") != null && pointDocument.poiCategory == "Wikipedia" && !feature.isPoint()) {
      return true;
    }

    insertPointToElasticsearch(pointDocument, docId);

    if ((pointDocument.poiIcon == "icon-peak" || pointDocument.poiIcon == "icon-river")
        && !isInterestingPoint(pointDocument)) {
      return true;
    }

    var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
        .setId(tileId);

    setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
    return true;
  }

  private void addNonIconFeaturesToElasricseach(SourceFeature feature) throws GeometryException {
    if (!feature.hasTag("name")) {
      return;
    }
    var pointDocument = new PointDocument();
    if (feature.hasTag("amenity", "place_of_worship") ||
        feature.hasTag("natural", "valley")) {
      pointDocument.poiIcon = "icon-search";
      pointDocument.poiIconColor = "black";
      pointDocument.poiCategory = "Other";
    }
    if (feature.hasTag("building") && !feature.hasTag("building", "no", "none", "No")) {
      pointDocument.poiIcon = "icon-search";
      pointDocument.poiIconColor = "black";
      pointDocument.poiCategory = "Other";
    }
    if (feature.hasTag("railway", "station") ||
        feature.hasTag("aerialway", "station")) {
      pointDocument.poiIcon = "icon-bus-stop";
      pointDocument.poiIconColor = "black";
      pointDocument.poiCategory = "Other";
    }
    if (feature.hasTag("natural", "ridge")) {
      pointDocument.poiIcon = "icon-peak";
      pointDocument.poiIconColor = "black";
      pointDocument.poiCategory = "Other";
    }
    if ((feature.hasTag("landuse", "recreation_ground") && feature.hasTag("sport", "mtb"))) {
      pointDocument.poiIcon = "icon-bike";
      pointDocument.poiIconColor = "green";
      pointDocument.poiCategory = "Bicycle";
    }
    if (feature.hasTag("landuse", "forest")) {
      pointDocument.poiIcon = "icon-tree";
      pointDocument.poiIconColor = "#008000";
      pointDocument.poiCategory = "Other";
    }

    if (pointDocument.poiIcon == null) {
      return;
    }

    if (pointDocument.poiIcon == "icon-search"
        && ((feature.getString("wikidata") != null || feature.getString("wikipedia") != null))) {
      pointDocument.poiIconColor = "black";
      pointDocument.poiIcon = "icon-wikipedia-w";
      pointDocument.poiCategory = "Wikipedia";
    }
    for (String language : supportedLanguages) {
      CoalesceIntoMap(pointDocument.name, language, feature.getString("name:" + language));
      CoalesceIntoMap(pointDocument.description, language, feature.getString("description:" + language));
    }
    if (feature.hasTag("name")) {
      CoalesceIntoMap(pointDocument.name, "default", feature.getString("name"));
    }
    if (feature.hasTag("description")) {
      CoalesceIntoMap(pointDocument.description, "default", feature.getString("description"));
    }
    // ADR-0011 item 1 — mirror of convertTagsToDocument's variant-name population for the
    // non-icon (building/station/forest/...) path, which builds its own PointDocument inline.
    addAltNames(pointDocument, feature);
    pointDocument.wikidata = feature.getString("wikidata");
    pointDocument.image = feature.getString("image");
    pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
    pointDocument.poiSource = "OSM";
    var docId = sourceFeatureToDocumentId(feature);
    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };
    // This path builds the document inline and (unlike convertTagsToDocument) never classified it,
    // so building/station/forest/natural=valley docs went in with feature_class=null. Classify here
    // so they carry the same class-match ranking signal as every other emit path.
    setFeatureClass(pointDocument, feature);
    insertPointToElasticsearch(pointDocument, docId);
  }

  /**
   * Safety floor for the insert path: every emitted document must carry a prominence so the
   * query-time field_value_factor multiply is consistent. A few emit paths (e.g. ski-lift ways,
   * relation-completion) don't run convertTagsToDocument, which would leave prominence null ->
   * field_value_factor missing:1.0 -> they'd unfairly beat a real, scored feature (whose prominence
   * is <1). Floor a null to the minimum prominence; leave any real value UNCHANGED. Package-private
   * static so it can be unit-tested directly (extracted, behavior-preserving).
   *
   * <p>Enforces ADR-0001 (build-time-vs-query-time prominence split): "every emitted doc must carry
   * a prominence value" — see docs/adr/0001-build-time-vs-query-time-prominence-split.md.
   */
  static float flooredProminence(Float prominence) {
    return prominence == null ? (float) ProminenceCalculator.FLOOR : prominence;
  }

  private void insertPointToElasticsearch(PointDocument pointDocument, String docId) {
    pointDocument.prominence = flooredProminence(pointDocument.prominence);
    emittedCount.increment();
    emittedPointsCount.increment();
    bulkIngester.add(BulkOperation.of(op -> op
        .index(idx -> idx
            .index(this.pointsIndexName)
            .id(docId)
            .document(pointDocument))));
  }

  private void insertBboxToElasticsearch(SourceFeature feature, String[] supportedLanguages) {
    Geometry polygon;
    try {
      polygon = GeoUtils.worldToLatLonCoords(feature.polygon());
    } catch (GeometryException e) {
      return;
    }
    try {
      var bbox = new BBoxDocument();
      bbox.area = feature.areaMeters();
      var lngLatCenterPoint = GeoUtils.worldToLatLonCoords(feature.centroid()).getCoordinate();
      bbox.center = new double[] { lngLatCenterPoint.getX(), lngLatCenterPoint.getY() };
      bbox.setBBox(polygon);
      for (String lang : supportedLanguages) {
        CoalesceIntoMap(bbox.name, lang, feature.getString("name:" + lang));
      }
      if (feature.hasTag("name")) {
        CoalesceIntoMap(bbox.name, "default", feature.getString("name"));
      }
      String bboxDocId = sourceFeatureToDocumentId(feature);
      emittedCount.increment();
      bulkIngester.add(BulkOperation.of(op -> op
          .index(idx -> idx
              .index(this.bboxIndexName)
              .id(bboxDocId)
              .document(bbox))));
    } catch (Exception e) {
      // Only geometry/serialization building can throw here now; the actual
      // indexing is handled (and its errors counted) by the bulk listener.
      LOGGER.warning(() -> "Failed to build bbox document for "
          + sourceFeatureToDocumentId(feature) + ": " + e.getMessage());
    }
  }

  /**
   * Flush every buffered document to Elasticsearch and wait for the in-flight
   * bulk requests to complete. Must be called after planetiler.run() finishes
   * emitting features and BEFORE the alias swap / refresh, so the new index is
   * fully populated when it goes live.
   *
   * close() already flushes and waits internally, but we flush explicitly first
   * (and log the remaining buffer) so the end-of-indexation flush is visible and
   * intentional rather than an implicit side effect of close().
   */
  public void flush() {
    LOGGER.info(() -> "Flushing final batch: " + bulkIngester.pendingOperations()
        + " buffered operations, " + bulkIngester.pendingRequests() + " in-flight requests.");
    bulkIngester.flush();
    // close() blocks until the flushed buffer and all in-flight requests (and
    // their listener callbacks, which update the counters) have completed.
    bulkIngester.close();
    LOGGER.info(() -> "Elasticsearch indexing finished: " + indexedCount.sum()
        + " documents indexed, " + getFailedCount() + " failed ("
        + failedPointsCount.sum() + " points, " + failedBboxCount.sum() + " bbox); "
        + getTransientCharges() + " transient charge(s) after retries exhausted ("
        + transientPointsCharges.sum() + " points, " + transientBboxCharges.sum() + " bbox).");
  }

  public long getIndexedCount() {
    return indexedCount.sum();
  }

  /** Total failed documents across all indices (points + bbox). Keeps the
   *  lossless invariant emitted == indexed + failed. */
  public long getFailedCount() {
    return failedPointsCount.sum() + failedBboxCount.sum();
  }

  /** Failed documents destined for the points index — these are missing SEARCH
   *  results and must break the build. */
  public long getFailedPointsCount() {
    return failedPointsCount.sum();
  }

  /** Failed bbox documents (geo_shape rejects on degenerate OSM geometries) —
   *  warn-only; they don't affect name search. */
  public long getFailedBboxCount() {
    return failedBboxCount.sum();
  }

  /** Points ops charged to the TRANSIENT bucket after retries were exhausted
   *  (client-side connection/socket timeouts, 429/5xx). Likely phantom; tolerated
   *  generously and backstopped by the reconcile gate. */
  public long getTransientPointsCharges() {
    return transientPointsCharges.sum();
  }

  /** Bbox ops charged to the transient bucket (warn-only analogue). */
  public long getTransientBboxCharges() {
    return transientBboxCharges.sum();
  }

  /** Total transient charges across both indices (points + bbox). */
  public long getTransientCharges() {
    return transientPointsCharges.sum() + transientBboxCharges.sum();
  }

  public long getEmittedCount() {
    return emittedCount.sum();
  }

  /** Emitted POINTS documents only (excludes bbox). Denominator for the points
   *  failure thresholds and the reconcile gate. */
  public long getEmittedPointsCount() {
    return emittedPointsCount.sum();
  }

  // ---------------------------------------------------------------------------
  // Step D — two-bucket indexing guard.
  //
  // Replaces the all-or-nothing `failedPointsCount > 0` predicate that, during the
  // incident, tripped exit-1 on 161 phantom transient timeouts even though the
  // whole-planet index had been restored. Two independent thresholds now apply to
  // POINTS:
  //   * GENUINE per-item data failures (4xx mapping/parse) — these really are
  //     missing search results, so the bar is strict: fail above
  //     max(50, 0.0001% of emitted).
  //   * TRANSIENT whole-batch charges (after retries exhausted) — likely phantom,
  //     so tolerate up to max(5000, 0.05% of emitted); fail only above that.
  // A real mass break (e.g. a mapping error dropping a big fraction) still fails
  // loudly via the genuine-failure bucket. The reconcile gate is the final backstop.
  // ---------------------------------------------------------------------------

  /** Strict threshold for GENUINE per-item points data failures. */
  static long genuineFailureThreshold(long emitted) {
    return Math.max(50L, (long) Math.ceil(emitted * 0.000001)); // 0.0001%
  }

  /** Generous threshold for TRANSIENT points charges (likely-phantom timeouts). */
  static long transientChargeThreshold(long emitted) {
    return Math.max(5_000L, (long) Math.ceil(emitted * 0.0005)); // 0.05%
  }

  /**
   * True when this run lost too many POINTS documents to treat the index as
   * complete. Two buckets with independent thresholds (see above): a strict bar on
   * genuine per-item data failures and a far more tolerant bar on transient
   * whole-batch charges. bbox failures (geo_shape rejects) are warn-only and never
   * trip this. Kept as a small pure predicate so the fail-the-build decision is
   * unit-testable without calling {@code System.exit}.
   */
  public boolean hasIndexingFailures() {
    long emittedPoints = emittedPointsCount.sum();
    return failedPointsCount.sum() > genuineFailureThreshold(emittedPoints)
        || transientPointsCharges.sum() > transientChargeThreshold(emittedPoints);
  }

  /**
   * Get the first point of the trail relation by checking some heuristics related
   * to the relation's first member
   * 
   * @param mergedLines - the merged lines helper
   * @return the first point of the trail relation
   * @throws GeometryException
   */
  private Point getFirstPointOfLineRelation(SourceFeature firstMemberFeature, SourceFeature secondMemberFeature)
      throws GeometryException {
    if (secondMemberFeature == null) {
      return GeoUtils.point(firstMemberFeature.worldGeometry().getCoordinate());
    }

    var firstMemberGeometry = (LineString) firstMemberFeature.line();
    var firstMemberStartCoordinate = firstMemberGeometry.getCoordinate();
    var firstMemberEndCoordinate = firstMemberGeometry.getCoordinateN(firstMemberGeometry.getNumPoints() - 1);
    var secondMemberGeometry = (LineString) secondMemberFeature.line();
    var secondMemberStartCoordinate = secondMemberGeometry.getCoordinate();
    var secondMemberEndCoordinate = secondMemberGeometry.getCoordinateN(secondMemberGeometry.getNumPoints() - 1);

    if (firstMemberStartCoordinate.equals2D(secondMemberStartCoordinate)
        || firstMemberStartCoordinate.equals2D(secondMemberEndCoordinate)) {
      return GeoUtils.point(firstMemberEndCoordinate);
    }
    if (firstMemberEndCoordinate.equals2D(secondMemberStartCoordinate)
        || firstMemberEndCoordinate.equals2D(secondMemberEndCoordinate)) {
      return GeoUtils.point(firstMemberStartCoordinate);
    }
    return GeoUtils.point(firstMemberStartCoordinate);
  }

  /**
   * This method removes relation members that are part of super relations and
   * have completed the ways processing.
   * This is done by checking for each new way that is being processed if it
   * completes a relation,
   * and remove that relation from the list of parent relations, this way at some
   * point all the ways and relations are empty
   * and it means we can continue processing them to add them to the database and
   * tiles.
   * It also keeps track of the first and second member features in case they are
   * needed to determine the first point of the relation.
   * 
   * @param feature
   */
  private void handleSuperRelationMembersUpdate(SourceFeature feature) {
    var removedElement = false;
    do {
      removedElement = false;
      for (var routeInfo : feature.relationInfo(RelationInfo.class, true)) {
        RelationInfo relation = routeInfo.relation();
        if (!relation.waysMemberIds.isEmpty() || !relation.RelationMemberIds.isEmpty()) {
          continue;
        }
        for (var superRouteInfo : feature.relationInfo(RelationInfo.class, true)) {
          RelationInfo superRelation = superRouteInfo.relation();
          if (!superRelation.isSuperRelation) {
            continue;
          }
          synchronized (superRelation) {
            if (superRelation.RelationMemberIds.remove(relation.id())) {
              superRelation.length += relation.length;
              removedElement = true;
              if (superRelation.firstMemberId == relation.id()) {
                superRelation.firstMemberFeature = relation.firstMemberFeature;
                superRelation.secondMemberFeature = relation.secondMemberFeature;
              }
            }
          }
        }
      }
    } while (removedElement);
  }

  private boolean isInterestingPoint(PointDocument pointDocument) {
    return !pointDocument.description.isEmpty() ||
        pointDocument.image != null;
  }

  private void setFeaturePropertiesFromPointDocument(Feature tileFeature, PointDocument pointDocument) {
    tileFeature.setAttr("wikidata", pointDocument.wikidata)
        .setAttr("wikimedia_commons", pointDocument.wikimedia_commons)
        .setAttr("image", pointDocument.image)
        .setAttr("website", pointDocument.website)
        .setAttr("poiIcon", pointDocument.poiIcon)
        .setAttr("poiIconColor", pointDocument.poiIconColor)
        .setAttr("poiCategory", pointDocument.poiCategory)
        .setAttr("poiSource", pointDocument.poiSource)
        .setAttr("poiLength", pointDocument.poiLength)
        .setAttr("poiDifficulty", pointDocument.poiDifficulty)
        .setZoomRange(8, 14)
        .setBufferPixels(0);
    for (String lang : supportedLanguages) {
      tileFeature.setAttr("name:" + lang, pointDocument.name.get(lang));
      tileFeature.setAttr("description:" + lang, pointDocument.description.get(lang));
    }
    if (pointDocument.name.containsKey("default")) {
      tileFeature.setAttr("name", pointDocument.name.get("default"));
    }
    if (pointDocument.description.containsKey("default")) {
      tileFeature.setAttr("description", pointDocument.description.get("default"));
    }
  }

  private boolean isBBoxFeature(SourceFeature feature, String[] supportedLanguages) {
    if (!feature.canBePolygon()) {
      return false;
    }
    var hasName = false;
    for (String language : supportedLanguages) {
      if (feature.hasTag("name:" + language)) {
        hasName = true;
        break;
      }
    }
    if (!feature.hasTag("name") && !hasName) {
      return false;
    }
    var isFeatureADecentCity = feature.hasTag("boundary", "administrative") &&
        feature.hasTag("admin_level") &&
        feature.getLong("admin_level") > 0 &&
        feature.getLong("admin_level") <= 8;
    if (isFeatureADecentCity) {
      return true;
    }
    if (feature.hasTag("place") &&
        !feature.hasTag("place", "suburb") &&
        !feature.hasTag("place", "neighbourhood") &&
        !feature.hasTag("place", "quarter") &&
        !feature.hasTag("place", "city_block") &&
        !feature.hasTag("place", "borough")) {
      return true;
    }
    if (feature.hasTag("landuse", "forest")) {
      return true;
    }
    return feature.hasTag("leisure", "nature_reserve") ||
        feature.hasTag("boundary", "national_park") ||
        feature.hasTag("boundary", "protected_area");
  }

  private String sourceFeatureToDocumentId(SourceFeature feature) {
    var tileId = feature.vectorTileFeatureId(config.featureSourceIdMultiplier());
    return "OSM_" + (String.valueOf(tileId).endsWith("1")
        ? "node_"
        : String.valueOf(tileId).endsWith("2")
            ? "way_"
            : "relation_")
        + feature.id();
  }

  private void setIconColorCategory(PointDocument pointDocument, WithTags feature) {
    if ("protected_area".equals(feature.getString("boundary")) ||
        "national_park".equals(feature.getString("boundary")) ||
        "nature_reserve".equals(feature.getString("leisure"))) {
      setProtectedAreaIcon(pointDocument, feature);
      return;
    }
    if (feature.getString("route") != null) {
      switch (feature.getString("route")) {
        case "hiking":
        case "foot":
          pointDocument.poiIconColor = "black";
          pointDocument.poiIcon = "icon-hike";
          pointDocument.poiCategory = "Hiking";
          return;
        case "bicycle":
        case "mtb":
          pointDocument.poiIconColor = "black";
          pointDocument.poiIcon = "icon-bike";
          pointDocument.poiCategory = "Bicycle";
          return;
        case "road":
          if ("yes".equals(feature.getString("scenic"))) {
            pointDocument.poiIconColor = "black";
            pointDocument.poiCategory = "4x4";
            pointDocument.poiIcon = "icon-four-by-four";
            return;
          }
      }
    }
    if (feature.getString("historic") != null) {
      pointDocument.poiIconColor = "#666666";
      pointDocument.poiCategory = "Historic";
      switch (feature.getString("historic")) {
        case "ruins":
          pointDocument.poiIcon = "icon-ruins";
          return;
        case "archaeological_site":
          pointDocument.poiIcon = "icon-archaeological";
          return;
        case "memorial":
        case "monument":
          pointDocument.poiIcon = "icon-memorial";
          return;
        case "tomb":
          pointDocument.poiIconColor = "black";
          pointDocument.poiIcon = "icon-cave";
          pointDocument.poiCategory = "Natural";
          return;
      }
    }
    if ("picnic_table".equals(feature.getString("leisure")) ||
        "picnic_site".equals(feature.getString("tourism")) ||
        "picnic".equals(feature.getString("amenity"))) {
      pointDocument.poiIconColor = "#734a08";
      pointDocument.poiIcon = "icon-picnic";
      pointDocument.poiCategory = "Camping";
      return;
    }

    if (feature.getString("natural") != null) {
      switch (feature.getString("natural")) {
        case "cave_entrance":
          pointDocument.poiIconColor = "black";
          pointDocument.poiIcon = "icon-cave";
          pointDocument.poiCategory = "Natural";
          return;
        case "spring":
          pointDocument.poiIconColor = "#1e80e3";
          pointDocument.poiIcon = "icon-tint";
          pointDocument.poiCategory = "Water";
          return;
        case "tree":
          pointDocument.poiIconColor = "#008000";
          pointDocument.poiIcon = "icon-tree";
          pointDocument.poiCategory = "Natural";
          return;
        case "flowers":
          pointDocument.poiIconColor = "#008000";
          pointDocument.poiIcon = "icon-flowers";
          pointDocument.poiCategory = "Natural";
          return;
        case "waterhole":
          pointDocument.poiIconColor = "#1e80e3";
          pointDocument.poiIcon = "icon-waterhole";
          pointDocument.poiCategory = "Water";
          return;
      }
    }

    if ("reservoir".equals(feature.getString("water")) ||
        "pond".equals(feature.getString("water")) ||
        "lake".equals(feature.getString("water")) ||
        "stream_pool".equals(feature.getString("water"))) {
      pointDocument.poiIconColor = "#1e80e3";
      pointDocument.poiIcon = "icon-tint";
      pointDocument.poiCategory = "Water";
      return;
    }

    if (feature.getString("man_made") != null) {
      pointDocument.poiIconColor = "#1e80e3";
      pointDocument.poiCategory = "Water";
      switch (feature.getString("man_made")) {
        case "water_well":
          pointDocument.poiIcon = "icon-water-well";
          return;
        case "cistern":
          pointDocument.poiIcon = "icon-cistern";
          return;
      }
    }

    if ("waterfall".equals(feature.getString("waterway"))) {
      pointDocument.poiIconColor = "#1e80e3";
      pointDocument.poiIcon = "icon-waterfall";
      pointDocument.poiCategory = "Water";
      return;
    }

    if ("waterway".equals(feature.getString("type"))) {
      pointDocument.poiIconColor = "#1e80e3";
      pointDocument.poiIcon = "icon-river";
      pointDocument.poiCategory = "Water";
      return;
    }

    if (feature.getString("place") != null) {
      pointDocument.poiIconColor = "black";
      pointDocument.poiIcon = "icon-home";
      pointDocument.poiCategory = "Wikipedia";
      return;
    }

    if (feature.getString("tourism") != null) {
      switch (feature.getString("tourism")) {
        case "viewpoint":
          pointDocument.poiIconColor = "#008000";
          pointDocument.poiIcon = "icon-viewpoint";
          pointDocument.poiCategory = "Viewpoint";
          return;
        case "camp_site":
          pointDocument.poiIconColor = "#734a08";
          pointDocument.poiIcon = "icon-campsite";
          pointDocument.poiCategory = "Camping";
          return;
        case "attraction":
          pointDocument.poiIconColor = "#ffb800";
          pointDocument.poiIcon = "icon-star";
          pointDocument.poiCategory = "Other";
          return;
        case "artwork":
          pointDocument.poiIconColor = "#ffb800";
          pointDocument.poiIcon = "icon-artwork";
          pointDocument.poiCategory = "Other";
          return;
        case "alpine_hut":
          pointDocument.poiIconColor = "#734a08";
          pointDocument.poiIcon = "icon-alpinehut";
          pointDocument.poiCategory = "Camping";
          return;
      }
    }

    // Recall fix (issue #11): treat natural=volcano like natural=peak so named
    // summit nodes tagged as volcanoes (e.g. Humphreys Peak, highest in AZ,
    // OSM node 359267393) get icon-peak instead of falling through to the
    // icon-search default, which causes processOtherSourceFeature to drop them
    // (same missing-summit class as Mount Rainier). setFeatureClass and
    // ProminenceCalculator already map volcano->peak; this completes that.
    if ("peak".equals(feature.getString("natural")) || "volcano".equals(feature.getString("natural"))) {
      pointDocument.poiIconColor = "black";
      pointDocument.poiIcon = "icon-peak";
      pointDocument.poiCategory = "Natural";
      return;
    }

    if (feature.getString("highway") != null) {
      switch (feature.getString("highway")) {
        case "cycleway":
          pointDocument.poiIconColor = "black";
          pointDocument.poiCategory = "Bicycle";
          pointDocument.poiIcon = "icon-bike";
          return;
        case "footway":
          pointDocument.poiIconColor = "black";
          pointDocument.poiCategory = "Hiking";
          pointDocument.poiIcon = "icon-hike";
          return;
        case "path":
          pointDocument.poiIconColor = "black";
          pointDocument.poiCategory = "Hiking";
          pointDocument.poiIcon = "icon-hike";
          return;
        case "track":
          pointDocument.poiIconColor = "black";
          pointDocument.poiCategory = "4x4";
          pointDocument.poiIcon = "icon-four-by-four";
          return;
      }
    }

    if ("place_of_worship".equals(feature.getString("amenity")) || "monastery".equals(feature.getString("amenity"))) {
      var religion = feature.getString("religion") != null ? feature.getString("religion") : "";
      pointDocument.poiCategory = "Other";
      pointDocument.poiIconColor = "black";
      switch (religion) {
        case "jewish":
          pointDocument.poiIcon = "icon-synagogue";
          return;
        case "christian":
          pointDocument.poiIcon = "icon-church";
          return;
        case "muslim":
          pointDocument.poiIcon = "icon-mosque";
          return;
        default:
          pointDocument.poiIcon = "icon-holy-place";
          return;
      }
    }

    if (feature.getString("ref:IL:inature") != null) {
      pointDocument.poiIconColor = "#116C00";
      pointDocument.poiIcon = "icon-inature";
      pointDocument.poiCategory = "iNature";
      return;
    }

    pointDocument.poiIconColor = "black";
    pointDocument.poiIcon = "icon-search";
    pointDocument.poiCategory = "Other";
  }

  /**
   * Differentiate protected areas by their PROTECTION TYPE so the search dropdown no longer shows the
   * same green leaf for every national park, monument, wilderness and forest (the "grand …" case:
   * Grand Canyon NP vs Grand Staircase-Escalante National Monument were both leaf). Keyed primarily
   * off the free-text {@code protection_title} (the most reliable discriminator on US data), with
   * {@code protect_class} and {@code boundary} as fallback. A plain national/state park or nature
   * reserve keeps the leaf (the established meaning). Only icomoon glyphs that exist in the web font
   * (src/fonts/icons.css) are used: icon-monument, icon-tree, icon-leaf.
   */
  private void setProtectedAreaIcon(PointDocument pointDocument, WithTags feature) {
    pointDocument.poiCategory = "Other";
    String title = feature.getString("protection_title");
    String t = title == null ? "" : title.toLowerCase(java.util.Locale.ROOT);
    String protectClass = feature.getString("protect_class");

    // National / Natural Monument — protect_class 3 is IUCN "Natural Monument".
    if (t.contains("monument") || "3".equals(protectClass)) {
      pointDocument.poiIconColor = "#734a08";
      pointDocument.poiIcon = "icon-monument";
      return;
    }
    // Wilderness (incl. "Wilderness Study Area") and forests — leafier green than a park boundary.
    if (t.contains("wilderness") || t.contains("forest") ||
        "forest".equals(feature.getString("boundary")) ||
        "national_forest".equals(feature.getString("boundary"))) {
      pointDocument.poiIconColor = "#006000";
      pointDocument.poiIcon = "icon-tree";
      return;
    }
    // Default: national/state park, nature reserve, protected landscape — keep the historical leaf.
    pointDocument.poiIconColor = "#008000";
    pointDocument.poiIcon = "icon-leaf";
  }

  /*
   * Hooks to override metadata values in the output mbtiles file. Only name is
   * required, the rest are optional. Bounds,
   * center, minzoom, maxzoom are set automatically based on input data and
   * planetiler config.
   *
   * See: https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata)
   */

  @Override
  public String name() {
    return "Trails POIs overlay";
  }

  @Override
  public String description() {
    return "Overlay for walking and bicycle routes";
  }

  @Override
  public boolean isOverlay() {
    return true; // when true sets type=overlay, otherwise type=baselayer
  }

  /*
   * Any time you use OpenStreetMap data, you must ensure clients display the
   * following copyright. Most clients will
   * display this automatically if you populate it in the attribution metadata in
   * the mbtiles file:
   */
  @Override
  public String attribution() {
    return """
        <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
        """.trim();
  }
}
