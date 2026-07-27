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
 * Picks the single representation to index for a place that appears as several
 * OSM elements (node, way, relation), so it stays searchable once and only
 * once.
 * Ranking, matching and dedup rely solely on OSM tags, element type and ids —
 * never on geometry.
 *
 * Polygons (ways / relations) are grouped by shared name / wikidata and deduped
 * in the first pass ({@link #recordWayIfNeeded}/{@link #recordRelationIfNeeded}):
 * the winner is the highest {@link PlaceRank}, ties broken by the lower OSM id. A
 * place node is not ranked here — it yields only to a polygon of the same place
 * that actually encloses it, which the caller checks against the container index.
 */
final class PlaceIndex {

  /** How strongly an OSM element represents its place, ordered weakest to strongest. */
  enum PlaceRank {
    NODE,
    PLAIN,
    RESIDENTIAL_WAY,
    RESIDENTIAL_RELATION,
    RELATION_WITH_NODE;
  }

  /** A ranked place representation; the better one is the higher rank, then the lower id. */
  private record Ranked(PlaceRank rank, long id) {
    boolean betterThan(Ranked other) {
      int byRank = rank.compareTo(other.rank);
      return byRank > 0 || (byRank == 0 && id < other.id);
    }
  }

  /** Place key ({@code name=…} / {@code wikidata=…}) -> the best representation seen. */
  private final Map<String, Ranked> bestByKey = new ConcurrentHashMap<>();
  /** Nodes that are members of a place relation (their location anchors it). */
  private final Set<Long> memberNodeIds = ConcurrentHashMap.newKeySet();
  /** Captured world coordinate {x, y} of each recorded member node. */
  private final Map<Long, double[]> memberNodeLocations = new ConcurrentHashMap<>();

  void recordWayIfNeeded(OsmElement.Way way) {
    if (isPlace(way)) {
      record(way, rankOf(way), way.id());
    }
  }

  void recordRelationIfNeeded(OsmElement.Relation relation) {
    if (!isPlace(relation)) {
      return;
    }
    for (var member : relation.members()) {
      if (isAnchorMember(member)) {
        memberNodeIds.add(member.ref());
      }
    }
    record(relation, rankOf(relation), relation.id());
  }

  /** The rank of a place way or relation from its tags, element type and members. */
  static PlaceRank rankOf(OsmElement element) {
    return switch (element) {
      case OsmElement.Way way -> way.hasTag("landuse", "residential") ? PlaceRank.RESIDENTIAL_WAY : PlaceRank.PLAIN;
      case OsmElement.Relation relation -> relation.members().stream().anyMatch(PlaceIndex::isAnchorMember)
          ? PlaceRank.RELATION_WITH_NODE
          : relation.hasTag("landuse", "residential") ? PlaceRank.RESIDENTIAL_RELATION : PlaceRank.PLAIN;
      default -> PlaceRank.NODE;
    };
  }

  /** A relation's settlement node: a node member with role {@code admin_centre} or {@code label}. */
  private static boolean isAnchorMember(OsmElement.Relation.Member member) {
    return member.type() == OsmElement.Type.NODE
        && ("admin_centre".equals(member.role()) || "label".equals(member.role()));
  }

  private void record(WithTags feature, PlaceRank rank, long id) {
    Ranked candidate = new Ranked(rank, id);
    for (String key : placeKeys(feature)) {
      bestByKey.merge(key, candidate, (current, cand) -> cand.betterThan(current) ? cand : current);
    }
  }

  /**
   * Whether this feature is the representation kept for its place: for every name
   * / wikidata it carries, it must be the best-ranked (ties: lowest id) element
   * recorded in the first pass.
   */
  boolean isWinner(SourceFeature feature) {
    return isWinner(placeRank(feature), feature.id(), placeKeys(feature));
  }

  /** Whether an element of this rank and id wins every one of its place keys. */
  boolean isWinner(PlaceRank rank, long id, Set<String> keys) {
    Ranked mine = new Ranked(rank, id);
    for (String key : keys) {
      Ranked best = bestByKey.get(key);
      if (best != null && !best.equals(mine)) {
        return false;
      }
    }
    return true;
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
  double[] anchorWorldLocation(SourceFeature feature) {
    if (!(feature instanceof OsmSourceFeature osm)
        || !(osm.originalElement() instanceof OsmElement.Relation relation)) {
      return null;
    }
    for (var member : relation.members()) {
      if (isAnchorMember(member)) {
        double[] location = memberNodeLocations.get(member.ref());
        if (location != null) {
          return location;
        }
      }
    }
    return null;
  }

  /** How strongly this feature represents its place (see the class javadoc). */
  static PlaceRank placeRank(SourceFeature feature) {
    if (!isPlace(feature) || feature.isPoint()) {
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
        && relation.members().stream().anyMatch(PlaceIndex::isAnchorMember);
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

  /** Every name the feature carries, across the default and supported languages. */
  static Set<String> placeNames(WithTags feature, String[] languages) {
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

  /** The keys a place is grouped by: its default name and its wikidata id. */
  static Set<String> placeKeys(WithTags feature) {
    var keys = new LinkedHashSet<String>();
    String name = feature.getString("name");
    if (name != null && !name.isBlank()) {
      keys.add("name=" + name);
    }
    String wikidata = feature.getString("wikidata");
    if (wikidata != null && !wikidata.isBlank()) {
      keys.add("wikidata=" + wikidata);
    }
    return keys;
  }

  private static boolean isPlace(WithTags feature) {
    String place = feature.getString("place");
    return place != null && !place.isBlank();
  }
}
