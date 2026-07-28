package il.org.osm.israelhiking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.onthegomap.planetiler.geo.GeoUtils;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.index.strtree.STRtree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

/**
 * An in-memory spatial index of container polygons, queried point-by-point to
 * find the places that enclose each POI. Built once (single-threaded) and then
 * queried concurrently from the Planetiler worker threads — {@link STRtree}
 * queries and {@link PreparedGeometry#contains} are both thread-safe once the
 * tree has been built.
 *
 * The
 * containers are the same documents the build writes to the bbox index,
 * so there is no separate store: a build {@link #load}s them from the live bbox
 * alias, which — until this build swaps its own bbox index in at the end —
 * still
 * points at the previous build's containers. Containers change rarely, so that
 * one-build lag is by design; a first-ever build finds no alias and tags
 * nothing.
 */
final class ContainerIndex {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerIndex.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private static final String SCROLL_KEEPALIVE = "2m";
  private static final int SCROLL_SIZE = 2000;

  /**
   * Two same-place polygons whose centres are within this distance are treated as
   * one even when neither encloses the other (their differently-shaped outlines
   * still describe the same settlement).
   */
  private static final double NEAR_METERS = 10_000;
  /**
   * Degrees to grow the point's query box so it never misses a candidate whose
   * centre is within {@link #NEAR_METERS} — deliberately over-estimated (uses a
   * short ~80 km/° so the box is a little large; actual distance is then checked
   * in metres).
   */
  private static final double NEAR_ENVELOPE_DEGREES = NEAR_METERS / 80_000.0;

  /**
   * A place a point can fall inside — an admin boundary, a settlement polygon, a
   * park. Carries only what point enrichment needs: the localized names, the
   * admin level (2 == country, 0 when the container is not an admin boundary),
   * the area in m² (used to pick the tightest container), and a simplified
   * polygon for the containment test.
   */
  static final class ContainerRecord {

    static final int COUNTRY_ADMIN_LEVEL = 2;

    final Map<String, String> names;
    final int adminLevel;
    final double area;
    final Geometry geometry;
    /**
     * The container's wikidata id, if any; used to match a place feature to its
     * polygon.
     */
    final String wikidata;
    /**
     * How strongly this polygon represents its place ({@code NONE} when it is not a
     * place at all).
     */
    final PlaceHelper.PlaceRank rank;
    /**
     * OSM element id; breaks ties between same-ranked polygons and excludes
     * self-containment.
     */
    final long id;
    /**
     * The polygon's centre's longitude; used for the "same place if centres are
     * close" test.
     */
    final double centerLng;
    /**
     * The polygon's centre's latitude; used for the "same place if centres are
     * close" test.
     */
    final double centerLat;

    ContainerRecord(Map<String, String> names, int adminLevel, double area, Geometry geometry, String wikidata,
        PlaceHelper.PlaceRank rank, long id, double centerLng, double centerLat) {
      this.names = names;
      this.adminLevel = adminLevel;
      this.area = area;
      this.geometry = geometry;
      this.wikidata = wikidata;
      this.rank = rank;
      this.id = id;
      this.centerLng = centerLng;
      this.centerLat = centerLat;
    }

    boolean isCountry() {
      return adminLevel == COUNTRY_ADMIN_LEVEL;
    }

    /**
     * Whether this is a place polygon representing the same place as the given
     * feature.
     */
    boolean isSamePlaceAs(Collection<String> otherNames, String otherWikidata) {
      if (rank == PlaceHelper.PlaceRank.NONE) {
        return false;
      }
      if (otherWikidata != null && otherWikidata.equals(wikidata)) {
        return true;
      }
      for (String name : names.values()) {
        if (otherNames.contains(name)) {
          return true;
        }
      }
      return false;
    }

    /**
     * Whether this polygon outranks a feature of the given rank and id (ties: lower
     * id).
     */
    boolean betterThan(PlaceHelper.PlaceRank otherRank, long otherId) {
      int byRank = rank.compareTo(otherRank);
      return byRank > 0 || (byRank == 0 && id < otherId);
    }
  }

  private record Entry(ContainerRecord record, PreparedGeometry prepared) {
  }

  private final STRtree tree = new STRtree();
  private final int loadedCount;

  ContainerIndex(Collection<ContainerRecord> records) {
    for (ContainerRecord record : records) {
      tree.insert(record.geometry.getEnvelopeInternal(),
          new Entry(record, PreparedGeometryFactory.prepare(record.geometry)));
    }
    tree.build();
    this.loadedCount = records.size();
  }

  /**
   * Loads the previous build's containers from the bbox alias; no alias yet
   * yields an empty index.
   */
  static ContainerIndex load(ElasticsearchClient esClient, String bboxAlias) throws IOException {
    if (!esClient.indices().existsAlias(a -> a.name(bboxAlias)).value()) {
      LOGGER.info("Container index: no '{}' index yet — this build tags no points", bboxAlias);
      return new ContainerIndex(List.of());
    }
    try {
      List<ContainerRecord> records = scroll(esClient, bboxAlias);
      LOGGER.info("Container index: loaded {} containers from '{}'", records.size(), bboxAlias);
      return new ContainerIndex(records);
    } catch (Exception e) {
      LOGGER.error("Container index: failed to load containers from '{}'", bboxAlias, e);
      return new ContainerIndex(List.of());
    }
  }

  /**
   * Whether a better-ranked polygon of the same place (shared name or wikidata)
   * already carries this point — because it either encloses the point or has its
   * centre within {@link #NEAR_METERS} of it. Uses the previous build's polygons,
   * so a brand-new place polygon starts deduping one build later.
   */
  boolean coveredByBetterPlace(double lat, double lng, Collection<String> names, String wikidata,
      PlaceHelper.PlaceRank rank, long id) {
    if (loadedCount == 0) {
      return false;
    }
    Coordinate coordinate = new Coordinate(lng, lat);
    Point point = GEOMETRY_FACTORY.createPoint(coordinate);
    Envelope box = new Envelope(coordinate);
    box.expandBy(NEAR_ENVELOPE_DEGREES);
    for (Object candidate : tree.query(box)) {
      Entry entry = (Entry) candidate;
      ContainerRecord record = entry.record();
      if (!record.isSamePlaceAs(names, wikidata) || !record.betterThan(rank, id)) {
        continue;
      }
      if (entry.prepared().contains(point)
          || GeoUtils.metersBetween(lng, lat, record.centerLng, record.centerLat) <= NEAR_METERS) {
        return true;
      }
    }
    return false;
  }

  /** The containers that enclose the given coordinate, in no particular order. */
  List<ContainerRecord> containing(double lat, double lng) {
    if (loadedCount == 0) {
      return List.of();
    }
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(lng, lat));
    List<ContainerRecord> hits = new ArrayList<>();
    for (Object candidate : tree.query(point.getEnvelopeInternal())) {
      Entry entry = (Entry) candidate;
      if (entry.prepared().contains(point)) {
        hits.add(entry.record());
      }
    }
    return hits;
  }

  /**
   * Tags the point with the places it falls inside: the union of their names
   * (for "point, place" search), plus the tightest enclosing place and the
   * country (for display). Uses the containers loaded from the previous build,
   * so a first-ever build tags nothing.
   */
  void enrich(PointDocument pointDocument, boolean isPlace) {
    if (pointDocument.location == null) {
      return;
    }
    var matches = containing(pointDocument.location[1], pointDocument.location[0]);
    if (matches.isEmpty()) {
      return;
    }
    ContainerRecord country = null;
    ContainerRecord container = null;
    Map<String, Set<String>> names = new LinkedHashMap<>();
    for (ContainerRecord match : matches) {
      match.names.forEach((lang, name) -> names.computeIfAbsent(lang, k -> new LinkedHashSet<>()).add(name));
      if (match.isCountry()) {
        if (country == null || match.area < country.area) {
          country = match;
        }
      } else if (!sharesNameForPlace(pointDocument, match, isPlace)
          && (container == null || match.area < container.area)) {
        container = match;
      }
    }
    Map<String, List<String>> parentNames = new LinkedHashMap<>();
    names.forEach((lang, set) -> parentNames.put(lang, new ArrayList<>(set)));
    pointDocument.poiParentNames = parentNames;
    if (country != null) {
      pointDocument.poiCountry = country.names;
    }
    if (container != null) {
      pointDocument.poiContainer = container.names;
    }
  }

  /**
   * Whether the container carries the same name as the point in any shared
   * language. A place node commonly sits in a polygon of the same name; using it
   * as the container would display "X, X", so skip it and let a wider place win.
   */
  private static boolean sharesNameForPlace(PointDocument pointDocument, ContainerRecord container, boolean isPlace) {
    if (!isPlace) {
      return false;
    }
    for (var entry : pointDocument.name.entrySet()) {
      if (entry.getValue().equals(container.names.get(entry.getKey()))) {
        return true;
      }
    }
    return false;
  }

  private static List<ContainerRecord> scroll(ElasticsearchClient esClient, String bboxAlias) throws IOException {
    List<ContainerRecord> records = new ArrayList<>();
    var response = esClient.search(s -> s
        .index(bboxAlias)
        .scroll(t -> t.time(SCROLL_KEEPALIVE))
        .size(SCROLL_SIZE)
        .query(q -> q.matchAll(m -> m)), JsonNode.class);
    String scrollId = response.scrollId();
    try {
      var hits = response.hits().hits();
      while (!hits.isEmpty()) {
        for (var hit : hits) {
          ContainerRecord record = toRecord(hit.source());
          if (record != null) {
            records.add(record);
          }
        }
        final String currentScrollId = scrollId;
        var scrollResponse = esClient.scroll(
            sc -> sc.scrollId(currentScrollId).scroll(t -> t.time(SCROLL_KEEPALIVE)), JsonNode.class);
        scrollId = scrollResponse.scrollId();
        hits = scrollResponse.hits().hits();
      }
    } finally {
      final String finalScrollId = scrollId;
      esClient.clearScroll(c -> c.scrollId(finalScrollId));
    }
    return records;
  }

  private static ContainerRecord toRecord(JsonNode source) {
    if (source == null) {
      return null;
    }
    JsonNode bbox = source.path("bbox");
    JsonNode nameNode = source.path("name");
    if (bbox.isMissingNode() || !nameNode.isObject() || nameNode.isEmpty()) {
      return null;
    }
    Map<String, String> names = new LinkedHashMap<>();
    nameNode.properties().forEach(field -> names.put(field.getKey(), field.getValue().asText()));
    try {
      Geometry geometry = geometryFromGeoJson(bbox);
      if (geometry == null || geometry.isEmpty()) {
        return null;
      }
      String wikidata = source.hasNonNull("wikidata") ? source.get("wikidata").asText() : null;
      PlaceHelper.PlaceRank rank = PlaceHelper.PlaceRank.fromOrdinal(source.path("placeRank").asInt(0));
      long id = source.path("id").asLong(0);
      JsonNode center = source.path("center");
      double centerLng = center.has(0) ? center.get(0).asDouble() : 0;
      double centerLat = center.has(1) ? center.get(1).asDouble() : 0;
      return new ContainerRecord(names, source.path("adminLevel").asInt(0), source.path("area").asDouble(0), geometry,
          wikidata, rank, id, centerLng, centerLat);
    } catch (RuntimeException e) {
      LOGGER.warn("Skipping a container with unreadable geometry: {}", e.getMessage());
      return null;
    }
  }

  private static Geometry geometryFromGeoJson(JsonNode geoJson) {
    String type = geoJson.path("type").asText();
    JsonNode coordinates = geoJson.path("coordinates");
    if (!coordinates.isArray()) {
      return null;
    }
    if ("polygon".equals(type)) {
      return polygon(coordinates);
    }
    if ("multipolygon".equals(type)) {
      Polygon[] polygons = new Polygon[coordinates.size()];
      for (int i = 0; i < coordinates.size(); i++) {
        polygons[i] = polygon(coordinates.get(i));
      }
      return GEOMETRY_FACTORY.createMultiPolygon(polygons);
    }
    return null;
  }

  private static Polygon polygon(JsonNode rings) {
    LinearRing shell = ring(rings.get(0));
    LinearRing[] holes = new LinearRing[rings.size() - 1];
    for (int i = 1; i < rings.size(); i++) {
      holes[i - 1] = ring(rings.get(i));
    }
    return GEOMETRY_FACTORY.createPolygon(shell, holes);
  }

  private static LinearRing ring(JsonNode coordinates) {
    Coordinate[] points = new Coordinate[coordinates.size()];
    for (int i = 0; i < coordinates.size(); i++) {
      JsonNode point = coordinates.get(i);
      points[i] = new Coordinate(point.get(0).asDouble(), point.get(1).asDouble());
    }
    return GEOMETRY_FACTORY.createLinearRing(points);
  }
}
