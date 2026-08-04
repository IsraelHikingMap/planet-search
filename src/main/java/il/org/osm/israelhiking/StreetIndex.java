package il.org.osm.israelhiking;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.onthegomap.planetiler.reader.WithTags;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;

/**
 * Merges the many OSM ways that make up one named street into a single
 * searchable record, without ever holding their geometries in memory. OSM
 * rarely groups a street's ways into a relation, so a street reaches the
 * profile
 * as dozens of unconnected named ways; keying each way by its name scoped to
 * the
 * settlement that contains it collapses them into one street, while keeping
 * "הרצל" in Haifa apart from "הרצל" in Netanya. Only the smallest OSM way id —
 * so
 * the document id maps back to a real, editable element — and that way's
 * document are kept per street; every other segment is discarded as it streams
 * by, so the footprint is one record per street rather than one per segment.
 *
 * A street's segments stream past on the worker threads, where the work is kept
 * to the minimum that can pick a winner: the container is looked up only as a
 * {@link ContainerIndex#tightestContainerScope scope handle}, and the kept
 * document is enriched with its containers' names only in {@link #flush}, once
 * per surviving street rather than once per segment.
 *
 * Streets are search only: this class feeds Elasticsearch, never the tile
 * layer.
 */
final class StreetIndex {

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

  /**
   * A named street scoped by the handle of its container, as it is keyed while
   * the input streams by — before any container has been named. A grid cell
   * stands in, as a negative scope so it can never be mistaken for a container,
   * when the street falls in none.
   */
  private record StreetCell(String name, long scope) {
  }

  /** A named street scoped to the settlement (or grid cell) that holds it. */
  record StreetKey(String name, String scope) {
  }

  /** Whether a highway tag value is a routable street this helper merges. */
  static boolean isStreetHighway(String highway) {
    return highway != null && STREET_HIGHWAYS.contains(highway);
  }

  /** Whether this feature is a named street line this helper should merge. */
  static boolean isStreet(WithTags feature) {
    return isStreetHighway(feature.getString("highway")) && feature.hasTag("name");
  }

  /** The single way kept per street: its minimal id and that way's document. */
  static final class StreetAggregator {
    private final long minId;
    private final PointDocument document;

    private StreetAggregator(long minId, PointDocument document) {
      this.minId = minId;
      this.document = document;
    }

    long minId() {
      return minId;
    }

    PointDocument document() {
      return document;
    }
  }

  private final ConcurrentHashMap<StreetCell, StreetAggregator> candidates = new ConcurrentHashMap<>();

  /**
   * Records one segment of a street — a document carrying only what its own tags
   * say — keyed by its name and the container it falls in, or the grid cell of
   * its point when it falls in none, keeping the segment with the smallest way
   * id.
   *
   * @param containerScope the handle of the container this segment falls in, or
   *                       0 for none; only its identity matters here, its names
   *                       are resolved in {@link #flush}
   */
  void add(long wayId, PointDocument document, long containerScope) {
    var key = new StreetCell(document.name.get("default"),
        containerScope != 0 ? containerScope : gridCell(document.location));
    candidates.compute(key, (k, current) -> {
      if (current == null || wayId < current.minId) {
        return new StreetAggregator(wayId, document);
      }
      return current;
    });
  }

  /** A grid-cell scope for a point that falls in no container. */
  private static long gridCell(double[] location) {
    long latCell = (long) Math.floor((location[1] + 90.0) / STREET_GRID_DEGREES);
    long lngCell = (long) Math.floor((location[0] + 180.0) / STREET_GRID_DEGREES);
    // Negative, so a cell can never collide with a container handle.
    return -(latCell * GRID_COLUMNS + lngCell) - 1;
  }

  /** The city the document was tagged with, or its grid cell when it has none. */
  private static StreetKey streetKey(PointDocument document) {
    if (document.poiContainer != null) {
      String city = document.poiContainer.get("default");
      if (city != null) {
        return new StreetKey(document.name.get("default"), city);
      }
    }
    return new StreetKey(document.name.get("default"), createGridCellKey(document.location));
  }

  /** A grid-cell scope for a point that falls in no container. */
  private static String createGridCellKey(double[] location) {
    long latCell = (long) Math.floor((location[1] + 90.0) / STREET_GRID_DEGREES);
    long lngCell = (long) Math.floor((location[0] + 180.0) / STREET_GRID_DEGREES);
    return "grid:" + latCell + ":" + lngCell;
  }

  /**
   * Emits one index operation per merged street, under its minimal way id, to
   * the given sink. Called from the finalize step, once the input pass is over
   * and every street's minimal id is known.
   *
   * This is where a kept street is enriched with the names of the places it
   * falls in — once per street rather than once per segment — and where the
   * streets are merged a second time, now by the container's name: a street
   * long enough to have been keyed by two different grid cells while streaming
   * by is one street again here, as long as both cells resolve to the same
   * place.
   */
  void flush(Consumer<BulkOperation> sink, String pointsIndex, Consumer<PointDocument> enricher) {
    candidates.values().parallelStream().forEach(candidate -> enricher.accept(candidate.document()));
    Map<StreetKey, StreetAggregator> streets = new HashMap<>();
    for (StreetAggregator candidate : candidates.values()) {
      streets.merge(streetKey(candidate.document()), candidate,
          (kept, other) -> kept.minId() <= other.minId() ? kept : other);
    }
    for (StreetAggregator street : streets.values()) {
      sink.accept(BulkOperation.of(op -> op
          .index(idx -> idx
              .index(pointsIndex)
              .id("OSM_way_" + street.minId())
              .document(street.document()))));
    }
  }
}
