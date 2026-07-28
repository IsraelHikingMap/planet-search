package il.org.osm.israelhiking;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmSourceFeature;

/**
 * Place helpers shared across the two passes: how strongly an element
 * represents
 * its place ({@link PlaceRank}, from tags and element type), its searchable
 * names, its estimated population, and — for a relation anchored by a
 * settlement
 * node — that node's location.
 *
 * Dedup itself is geometric and lives in {@link ContainerIndex}: a place
 * feature
 * (node or polygon) is dropped when its representative point falls inside a
 * better-ranked polygon of the same place. This class only supplies the rank,
 * names and anchor that decision needs.
 */
final class PlaceHelper {

  /**
   * How strongly an OSM element represents its place, ordered weakest to
   * strongest.
   */
  enum PlaceRank {
    /**
     * Not a place at all — a plain container (forest, boundary) that never
     * represents a place.
     */
    NONE,
    NODE,
    PLAIN,
    RESIDENTIAL_WAY,
    RESIDENTIAL_RELATION,
    RELATION_WITH_NODE;

    /**
     * The rank for the given ordinal, or {@link #NONE} for anything out of range.
     */
    static PlaceRank fromOrdinal(int ordinal) {
      PlaceRank[] values = values();
      return ordinal >= 0 && ordinal < values.length ? values[ordinal] : NONE;
    }
  }

  /** Nodes that are members of a place relation (their location anchors it). */
  private final Set<Long> memberNodeIds = ConcurrentHashMap.newKeySet();
  /** Captured world coordinate {x, y} of each recorded member node. */
  private final Map<Long, double[]> memberNodeLocations = new ConcurrentHashMap<>();

  /**
   * Remembers a place relation's settlement node members so their location can
   * anchor it.
   */
  void recordRelationIfNeeded(OsmElement.Relation relation) {
    if (!isPlace(relation)) {
      return;
    }
    for (var member : relation.members()) {
      if (isLabelNodeMember(member)) {
        memberNodeIds.add(member.ref());
      }
    }
  }

  /**
   * A relation's settlement node: a node member with role {@code admin_centre} or
   * {@code label}.
   */
  private static boolean isLabelNodeMember(OsmElement.Relation.Member member) {
    return member.type() == OsmElement.Type.NODE
        && ("admin_centre".equals(member.role()) || "label".equals(member.role()));
  }

  /**
   * Remember a node's world location if some place relation is anchored by it.
   */
  void captureMemberNode(long nodeId, double worldX, double worldY) {
    if (memberNodeIds.contains(nodeId)) {
      memberNodeLocations.put(nodeId, new double[] { worldX, worldY });
    }
  }

  /**
   * The world coordinate to place this feature's point at when it is a relation
   * anchored by a node member (the member's location), or null to fall back to
   * the feature's own geometry.
   */
  double[] getLabelNodeWorldLocation(SourceFeature feature) {
    if (!(feature instanceof OsmSourceFeature osm)
        || !(osm.originalElement() instanceof OsmElement.Relation relation)) {
      return null;
    }
    for (var member : relation.members()) {
      if (isLabelNodeMember(member)) {
        double[] location = memberNodeLocations.get(member.ref());
        if (location != null) {
          return location;
        }
      }
    }
    return null;
  }

  /** How strongly this feature represents its place (see the class javadoc). */
  static PlaceRank calculatePlaceRank(SourceFeature feature) {
    if (!isPlace(feature)) {
      return PlaceRank.NONE;
    }
    if (feature.isPoint()) {
      return PlaceRank.NODE;
    }
    boolean relation = feature instanceof OsmSourceFeature osm && osm.originalElement() instanceof OsmElement.Relation;
    if (relation && hasNodeMember(feature)) {
      return PlaceRank.RELATION_WITH_NODE;
    }
    if (feature.hasTag("landuse", "residential")) {
      return relation ? PlaceRank.RESIDENTIAL_RELATION : PlaceRank.RESIDENTIAL_WAY;
    }
    return PlaceRank.PLAIN;
  }

  /**
   * Whether this relation carries a settlement node member (role admin_centre or
   * label), whose location anchors the relation's point.
   */
  static boolean hasNodeMember(SourceFeature feature) {
    return feature instanceof OsmSourceFeature osm
        && osm.originalElement() instanceof OsmElement.Relation relation
        && relation.members().stream().anyMatch(PlaceHelper::isLabelNodeMember);
  }

  /**
   * The population to index for a place: the parsed {@code population} tag when
   * present, otherwise a rough default from the place kind. Empty for a feature
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
   * Every name the feature carries, across the default and supported languages.
   */
  static Set<String> getPlaceNames(WithTags feature, String[] languages) {
    var names = new LinkedHashSet<String>();
    if (feature.hasTag("name")) {
      names.add(feature.getString("name"));
    }
    for (String language : languages) {
      if (feature.hasTag("name:" + language)) {
        names.add(feature.getString("name:" + language));
      }
    }
    return names;
  }

  private static boolean isPlace(WithTags feature) {
    String place = feature.getString("place");
    return place != null && !place.isBlank();
  }
}
