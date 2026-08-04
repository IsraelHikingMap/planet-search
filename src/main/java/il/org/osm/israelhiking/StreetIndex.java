package il.org.osm.israelhiking;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongLongHashMap;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;

/**
 * Merges the many OSM ways that make up one named street into a single
 * searchable record. OSM rarely groups a street's ways into a relation, so a
 * street reaches the profile as dozens of unconnected named ways; keying each
 * way by its name scoped to the settlement that contains it collapses them into
 * one street, while keeping "הרצל" in Haifa apart from "הרצל" in Netanya. The
 * smallest OSM way id wins, so the document id maps back to a real, editable
 * element.
 *
 * The merge cannot finish until the last segment has streamed by, and there are
 * tens of millions of streets on the planet — so what is held between the two
 * has to be small. This keeps two longs per street: the winning way id and its
 * point, packed. Names are held as 64-bit hashes rather than strings, and no
 * document is built during the input pass at all.
 *
 * The documents are built in {@link #flush}, from a second read of the OSM
 * input that decodes only the ways that won. That read needs no node locations
 * — every street's point was already recorded — so it is a sequential scan of
 * the input, not a second planetiler pass.
 *
 * Streets are search only: this class feeds Elasticsearch, never the tile
 * layer.
 */
final class StreetIndex {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreetIndex.class);

  private static final Set<String> STREET_HIGHWAYS = Set.of(
      "motorway", "trunk", "primary", "secondary", "tertiary",
      "unclassified", "residential", "living_street", "pedestrian", "road",
      "motorway_link", "trunk_link", "primary_link", "secondary_link", "tertiary_link");

  /**
   * A street outside any settlement has no container to scope its name by, so
   * its name is scoped by the grid cell of its representative point instead —
   * this keeps two same-named rural roads apart while still joining a single
   * road's segments.
   */
  private static final double STREET_GRID_DEGREES = 0.05;

  /**
   * Grid cells per row of latitude, to fold a cell into a single number. One
   * past the last cell, so the half-open cell at longitude 180 does not fold
   * onto the first cell of the next row.
   */
  private static final long GRID_COLUMNS = Math.round(360.0 / STREET_GRID_DEGREES) + 1;

  /** Fixed-point degrees — OSM's own coordinate precision, about a centimetre. */
  private static final double COORDINATE_SCALE = 1e7;

  /** How long the second read may take before the build gives up on it. */
  private static final long RESCAN_TIMEOUT_HOURS = 6;

  /**
   * A named street scoped to the settlement that holds it, both as 64-bit
   * hashes: at tens of millions of streets, holding the two strings instead is
   * gigabytes.
   */
  private record StreetKey(long name, long scope) {
  }

  /** The way that represents a street: the smallest id seen, and its point. */
  private record StreetWinner(long wayId, long coordinate) {
  }

  /** Whether a highway tag value is a routable street this helper merges. */
  static boolean isStreetHighway(String highway) {
    return highway != null && STREET_HIGHWAYS.contains(highway);
  }

  /** Whether this feature is a named street line this helper should merge. */
  static boolean isStreet(WithTags feature) {
    return isStreetHighway(feature.getString("highway")) && feature.hasTag("name");
  }

  private final ConcurrentHashMap<StreetKey, StreetWinner> winners = new ConcurrentHashMap<>();

  /**
   * Segments seen, to report against the number of streets kept — the gap is
   * what this merge exists to collapse.
   */
  private final LongAdder segments = new LongAdder();

  private final BulkIndexer bulkIndexer;
  private final String pointsIndex;
  private final Path osmPath;
  private final int threads;
  private final PointDocumentFactory documents;

  /**
   * @param bulkIndexer where the merged street documents are handed to be
   *                    indexed
   * @param pointsIndex the index they are written to
   * @param osmPath     the OSM input, read again in {@link #flush} to build the
   *                    documents of the streets that won
   * @param threads     how many threads that read may decode blocks on
   * @param documents   builds a street's document from the way that represents
   *                    it
   */
  StreetIndex(BulkIndexer bulkIndexer, String pointsIndex, Path osmPath, int threads,
      PointDocumentFactory documents) {
    this.bulkIndexer = bulkIndexer;
    this.pointsIndex = pointsIndex;
    this.osmPath = osmPath;
    this.threads = threads;
    this.documents = documents;
  }

  /**
   * Records one segment of a street, keeping the segment with the smallest way
   * id per street. Nothing but that id and the point is kept: the document is
   * built later, in {@link #flush}.
   *
   * @param containerName the name of the settlement the segment falls in, or
   *                      null when it falls in none — then the point's grid
   *                      cell scopes the name instead
   */
  void add(long wayId, String name, String containerName, double lng, double lat) {
    segments.increment();
    var key = new StreetKey(hash(name), containerName != null ? hash(containerName) : gridCell(lng, lat));
    long coordinate = packCoordinate(lng, lat);
    winners.compute(key, (k, current) -> current == null || wayId < current.wayId()
        ? new StreetWinner(wayId, coordinate)
        : current);
  }

  /**
   * Builds and emits one document per merged street, by reading the OSM input
   * again and decoding only the ways that won their street. Called from the
   * finalize step, once the input pass is over and every street's minimal id is
   * known.
   */
  void flush() throws IOException {
    int held = winners.size();
    LOGGER.info("Street index: {} street segments merged into {} streets held in memory (heap used ~{} MB)",
        segments.sum(), held, usedHeapMegabytes());
    if (held == 0) {
      return;
    }
    var coordinateByWayId = new LongLongHashMap(held);
    for (StreetWinner winner : winners.values()) {
      coordinateByWayId.put(winner.wayId(), winner.coordinate());
    }
    winners.clear();

    if (!Files.exists(osmPath)) {
      LOGGER.error("Street index: {} is no longer there, so {} streets cannot be built and go unindexed",
          osmPath, held);
      return;
    }
    rescan(coordinateByWayId);
  }

  /**
   * Reads the OSM input and builds a document for every way that represents a
   * street. Blocks are decoded on a bounded pool — the queue is small on
   * purpose, so a slow consumer stalls the reader rather than letting undecoded
   * blocks pile up in memory, which is the thing this whole class is avoiding.
   */
  private void rescan(LongLongHashMap coordinateByWayId) throws IOException {
    long startTime = System.currentTimeMillis();
    var emitted = new LongAdder();
    var executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(threads * 2), new ThreadPoolExecutor.CallerRunsPolicy());
    try (var blocks = new OsmInputFile(osmPath).get()) {
      blocks.forEachBlock(block -> executor.execute(() -> {
        for (OsmElement element : block.decodeElements()) {
          if (!(element instanceof OsmElement.Way way) || !coordinateByWayId.containsKey(way.id())) {
            continue;
          }
          long coordinate = coordinateByWayId.get(way.id());
          var document = documents.buildStreetDocument(way, longitudeOf(coordinate), latitudeOf(coordinate));
          this.bulkIndexer.add(BulkOperation.of(op -> op
              .index(idx -> idx
                  .index(pointsIndex)
                  .id("OSM_way_" + way.id())
                  .document(document))));
          emitted.increment();
        }
      }));
      executor.shutdown();
      if (!executor.awaitTermination(RESCAN_TIMEOUT_HOURS, TimeUnit.HOURS)) {
        LOGGER.error("Street index: reading {} did not finish in {}h, some streets go unindexed",
            osmPath, RESCAN_TIMEOUT_HOURS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while reading " + osmPath + " for the street documents", e);
    } finally {
      executor.shutdownNow();
    }
    LOGGER.info("Street index: built {} street documents from {} in {}ms (heap used ~{} MB)",
        emitted.sum(), osmPath, System.currentTimeMillis() - startTime, usedHeapMegabytes());
  }

  /** The way ids that represent the merged streets, smallest first. */
  long[] winningWayIds() {
    return winners.values().stream().mapToLong(StreetWinner::wayId).sorted().toArray();
  }

  /**
   * A 64-bit FNV-1a hash of a name, so a street key holds two longs instead of
   * two strings. Over tens of millions of streets a 64-bit collision — two
   * unrelated streets merged into one — is vanishingly unlikely, and a grid
   * cell landing on a name's hash more so, since cells occupy a narrow band of
   * small negative numbers.
   */
  private static long hash(String value) {
    long hash = 0xcbf29ce484222325L;
    for (int i = 0; i < value.length(); i++) {
      hash = (hash ^ value.charAt(i)) * 0x100000001b3L;
    }
    return hash;
  }

  /** A grid-cell scope for a point that falls in no container. */
  private static long gridCell(double lng, double lat) {
    long latCell = (long) Math.floor((lat + 90.0) / STREET_GRID_DEGREES);
    long lngCell = (long) Math.floor((lng + 180.0) / STREET_GRID_DEGREES);
    // Negative, to stay clear of the hashes of the containers that have a name.
    return -(latCell * GRID_COLUMNS + lngCell) - 1;
  }

  /** Both ordinates in one long, as fixed-point degrees. */
  private static long packCoordinate(double lng, double lat) {
    return (Math.round(lng * COORDINATE_SCALE) << 32) | (Math.round(lat * COORDINATE_SCALE) & 0xffffffffL);
  }

  private static double longitudeOf(long coordinate) {
    return (int) (coordinate >> 32) / COORDINATE_SCALE;
  }

  private static double latitudeOf(long coordinate) {
    return (int) coordinate / COORDINATE_SCALE;
  }

  /** Heap in use, to size what the merge actually costs on a planet build. */
  private static long usedHeapMegabytes() {
    var runtime = Runtime.getRuntime();
    return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
  }
}
