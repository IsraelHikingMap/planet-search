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
 * Tracks how each place is represented across the OSM node / way / relation
 * forms seen in the first pass, so the second pass can index every place exactly
 * once under the ranking relation then node then way — while only collapsing
 * representations that are the same place, never two unrelated places that
 * merely share a name. A shared name (or wikidata) is the fast first filter for
 * "maybe the same place"; a structural check then confirms it:
 * - a node is absorbed by a relation only when it is a member (child) of that
 * relation;
 * - a way is absorbed by a node only when the way's polygon contains that node.
 * The winning representation is indexed with its own tags, so whatever a place
 * looks like in OSM is what shows up here.
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
  /** Node id to the name=/wikidata= keys of the place relations it is a member of. */
  private final Map<Long, Set<String>> memberNodeRelationKeys = new ConcurrentHashMap<>();

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
   * Records a place relation and the nodes it contains as members, but only when
   * planetiler will turn it into a polygon in the second pass — a polygonal type
   * with a way member, mirroring its own multipolygon test. Recording one that
   * never materializes would suppress the node that still represents the place.
   */
  void recordRelation(OsmElement.Relation relation) {
    if (!relation.hasTag("place")) {
      return;
    }
    boolean resolvesToPolygon = relation.hasTag("type", "multipolygon", "boundary", "land_area")
        && relation.members().stream().anyMatch(member -> member.type() == OsmElement.Type.WAY);
    if (!resolvesToPolygon) {
      return;
    }
    List<String> keys = placeKeys(relation);
    if (keys.isEmpty()) {
      return;
    }
    for (var member : relation.members()) {
      if (member.type() == OsmElement.Type.NODE) {
        memberNodeRelationKeys.computeIfAbsent(member.ref(), k -> ConcurrentHashMap.newKeySet()).addAll(keys);
      }
    }
  }

  /**
   * Whether this second-pass feature is the one to index for its place, applying
   * the ranking relation then node then way: a relation always wins; a node
   * yields only to a relation of the same place it is a member of; a way yields
   * only to a place node of the same place that falls inside it. Structurally
   * unrelated same-named places therefore both survive.
   */
  boolean shouldIndex(SourceFeature feature) throws GeometryException {
    if (feature.isPoint()) {
      return !isMemberOfMatchingRelation(feature);
    }
    if (feature instanceof OsmSourceFeature osm && osm.originalElement() instanceof OsmElement.Relation) {
      return true;
    }
    return !containsMatchingNode(feature);
  }

  /** Whether this node is a member of a place relation that shares its name or wikidata. */
  private boolean isMemberOfMatchingRelation(SourceFeature node) {
    Set<String> relationKeys = memberNodeRelationKeys.get(node.id());
    if (relationKeys == null) {
      return false;
    }
    for (String key : placeKeys(node)) {
      if (relationKeys.contains(key)) {
        return true;
      }
    }
    return false;
  }

  /** Whether the way's polygon contains a place node that shares its name or wikidata. */
  private boolean containsMatchingNode(SourceFeature way) throws GeometryException {
    if (!way.canBePolygon()) {
      return false;
    }
    Geometry polygon = null;
    for (String key : placeKeys(way)) {
      List<Long> ids = nodeIdsByKey.get(key);
      if (ids == null) {
        continue;
      }
      if (polygon == null) {
        polygon = way.latLonGeometry();
      }
      for (long id : ids) {
        PlaceNode node = nodesById.get(id);
        if (node != null && polygon.covers(GeoUtils.point(node.lon(), node.lat()))) {
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
