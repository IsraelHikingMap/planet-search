package il.org.osm.israelhiking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.locationtech.jts.geom.Geometry;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmSourceFeature;

/**
 * Tracks the place nodes and place-relation wikidata seen in the first pass, so
 * the second pass indexes each place exactly once under the ranking
 * relation then node then way:
 * - a node yields to a place relation that shares its wikidata, so the relation
 * wins; wikidata is unique per entity, so unrelated places never collapse;
 * - a way yields to a place node of the same place (name or wikidata) that falls
 * inside it, so the node wins over a bare area;
 * - a relation always wins.
 *
 * A node and a relation can only be linked at node-processing time through a tag
 * they both carry (wikidata) — geometry-based containment is not available then,
 * because relations are processed after nodes. So an unlinked node + relation
 * pair is indexed twice until they are given a shared wikidata in OSM.
 *
 * Populated from preprocessOsm* on pass 1 and queried from processFeature on
 * pass 2; both run multi-threaded, so the stores are concurrency-safe.
 */
final class PlaceIndex {

  /** A place node's location, kept by OSM id for the way-containment test. */
  private record PlaceNode(double lon, double lat) {
  }

  private final Map<Long, PlaceNode> nodesById = new ConcurrentHashMap<>();
  /** name= / wikidata= key to the ids of the place nodes carrying it, for containment candidates. */
  private final Map<String, List<Long>> nodeIdsByKey = new ConcurrentHashMap<>();
  /** "wikidata=..." keys of place relations that resolve to a polygon; a node with one yields to it. */
  private final Set<String> relationWikidataKeys = ConcurrentHashMap.newKeySet();

  void recordNode(OsmElement.Node node, String[] languages) {
    if (!node.hasTag("place") || !OsmNames.hasSearchableName(node, languages)) {
      return;
    }
    nodesById.put(node.id(), new PlaceNode(node.lon(), node.lat()));
    for (String key : placeKeys(node)) {
      nodeIdsByKey.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>())).add(node.id());
    }
  }

  /**
   * Records a place relation's wikidata, but only when planetiler will turn the
   * relation into a polygon in the second pass — a polygonal type with a way
   * member, mirroring its own multipolygon test. Recording one that never
   * materializes would suppress the node that still represents the place.
   */
  void recordRelation(OsmElement.Relation relation) {
    var wikidata = relation.getString("wikidata");
    if (!relation.hasTag("place") || wikidata == null) {
      return;
    }
    boolean resolvesToPolygon = relation.hasTag("type", "multipolygon", "boundary", "land_area")
        && relation.members().stream().anyMatch(member -> member.type() == OsmElement.Type.WAY);
    if (resolvesToPolygon) {
      relationWikidataKeys.add("wikidata=" + wikidata);
    }
  }

  /**
   * Whether this second-pass feature is the one to index for its place: a node
   * yields to a relation that shares its wikidata, a way yields to a place node
   * of the same place that falls inside it, and a relation always wins.
   */
  boolean shouldIndex(SourceFeature feature) throws GeometryException {
    if (feature.isPoint()) {
      var wikidata = feature.getString("wikidata");
      return wikidata == null || !relationWikidataKeys.contains("wikidata=" + wikidata);
    }
    if (feature instanceof OsmSourceFeature osm && osm.originalElement() instanceof OsmElement.Relation) {
      return true;
    }
    return !containsMatchingNode(feature);
  }

  /** Whether the way's polygon contains a place node that shares its name or wikidata. */
  private boolean containsMatchingNode(SourceFeature way) throws GeometryException {
    if (!way.canBePolygon()) {
      return false;
    }
    Geometry geometry = null;
    for (String key : placeKeys(way)) {
      List<Long> ids = nodeIdsByKey.get(key);
      if (ids == null) {
        continue;
      }
      if (geometry == null) {
        geometry = way.latLonGeometry();
      }
      for (long id : ids) {
        PlaceNode node = nodesById.get(id);
        if (node != null && geometry.covers(GeoUtils.point(node.lon(), node.lat()))) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * The population to index for a place: the parsed {@code population} tag when
   * present, otherwise a rough default from the place rank. Empty for a feature
   * with no {@code place} tag, so callers leave non-places untouched.
   */
  static OptionalInt estimatePopulation(WithTags feature) {
    String place = feature.getString("place");
    if (place == null || place.isBlank()) {
      return OptionalInt.empty();
    }
    var parsed = OsmNumberParser.parsePopulation(feature.getString("population"));
    if (parsed.isPresent()) {
      return parsed;
    }
    return OptionalInt.of(switch (place) {
      case "city" -> 1_000_000;
      case "town" -> 50_000;
      case "village" -> 2_000;
      case "hamlet" -> 200;
      default -> 20;
    });
  }

  /**
   * The "name=..." and "wikidata=..." keys a place feature is indexed under.
   * Wikidata is unique per entity, so it links a node and polygon even when
   * their names differ slightly.
   */
  static List<String> placeKeys(WithTags feature) {
    var keys = new ArrayList<String>(2);
    if (feature.hasTag("name")) {
      keys.add("name=" + feature.getString("name"));
    }
    var wikidata = feature.getString("wikidata");
    if (wikidata != null) {
      keys.add("wikidata=" + wikidata);
    }
    return keys;
  }
}
