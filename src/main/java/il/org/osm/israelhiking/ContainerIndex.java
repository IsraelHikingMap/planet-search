package il.org.osm.israelhiking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import org.locationtech.jts.geom.Coordinate;
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

    ContainerRecord(Map<String, String> names, int adminLevel, double area, Geometry geometry) {
      this.names = names;
      this.adminLevel = adminLevel;
      this.area = area;
      this.geometry = geometry;
    }

    boolean isCountry() {
      return adminLevel == COUNTRY_ADMIN_LEVEL;
    }
  }

  private record Entry(ContainerRecord record, PreparedGeometry prepared) {
  }

  private final STRtree tree = new STRtree();
  private final int loadedCount;

  private ContainerIndex(Collection<ContainerRecord> records) {
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
      return new ContainerRecord(names, source.path("adminLevel").asInt(0), source.path("area").asDouble(0), geometry);
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
