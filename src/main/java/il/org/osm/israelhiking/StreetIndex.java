package il.org.osm.israelhiking;

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

  private final ConcurrentHashMap<StreetKey, StreetAggregator> streets = new ConcurrentHashMap<>();

  /**
   * Records one segment of a street — an already-enriched document — keyed by
   * its name and the city (container) it was tagged with, or the grid cell of
   * its point when it has no container, keeping the segment with the smallest
   * way id.
   */
  void add(long wayId, PointDocument document) {
    var key = new StreetKey(document.name.get("default"), getStreetKey(document));
    streets.compute(key, (k, current) -> {
      if (current == null || wayId < current.minId) {
        return new StreetAggregator(wayId, document);
      }
      return current;
    });
  }

  /** The city the document was tagged with, or its grid cell when it has none. */
  private static String getStreetKey(PointDocument document) {
    if (document.poiContainer != null) {
      String city = document.poiContainer.get("default");
      if (city != null) {
        return city;
      }
    }
    return createGridCellKey(document.location);
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
   * and every street's minimal id is known; the documents were built and
   * enriched while their segments streamed by.
   */
  void flush(Consumer<BulkOperation> sink, String pointsIndex) {
    for (StreetAggregator street : streets.values()) {
      sink.accept(BulkOperation.of(op -> op
          .index(idx -> idx
              .index(pointsIndex)
              .id("OSM_way_" + street.minId())
              .document(street.document()))));
    }
  }
}
