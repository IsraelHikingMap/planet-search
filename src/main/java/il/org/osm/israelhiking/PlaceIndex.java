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
 * Rank of a representation, highest first:
 * 3 — a relation anchored by a node member (its point becomes that node);
 * 2 — a landuse=residential relation;
 * 1 — a landuse=residential way;
 * 0 — a plain place polygon;
 * -1 — a place node.
 *
 * Representations are grouped by shared name / wikidata; the winner is the
 * highest rank, ties broken by the lower OSM id. Everything is decided in the
 * first pass ({@link #recordNode}/{@link #recordWay}/{@link #recordRelation}),
 * so the second pass ({@link #isWinner}) is a same-build lookup with no lag.
 */
final class PlaceIndex {

  static final int RANK_NODE = -1;
  static final int RANK_PLAIN = 0;
  static final int RANK_RESIDENTIAL_WAY = 1;
  static final int RANK_RESIDENTIAL_RELATION = 2;
  static final int RANK_RELATION_WITH_NODE = 3;

  /** Place key ({@code name=…} / {@code wikidata=…}) -> best {rank, id} seen. */
  private final Map<String, long[]> bestByKey = new ConcurrentHashMap<>();
  /** Nodes that are members of a place relation (their location anchors it). */
  private final Set<Long> memberNodeIds = ConcurrentHashMap.newKeySet();
  /** Captured world coordinate {x, y} of each recorded member node. */
  private final Map<Long, double[]> memberNodeLocations = new ConcurrentHashMap<>();

  void recordNodeIfNeeded(OsmElement.Node node) {
    if (isPlace(node)) {
      record(node, RANK_NODE, node.id());
    }
  }

  void recordWayIfNeeded(OsmElement.Way way) {
    if (isPlace(way)) {
      record(way, way.hasTag("landuse", "residential") ? RANK_RESIDENTIAL_WAY : RANK_PLAIN, way.id());
    }
  }

  void recordRelationIfNeeded(OsmElement.Relation relation) {
    if (!isPlace(relation)) {
      return;
    }
    boolean hasNodeMember = false;
    for (var member : relation.members()) {
      if (member.type() == OsmElement.Type.NODE) {
        hasNodeMember = true;
        memberNodeIds.add(member.ref());
      }
    }
    int rank = hasNodeMember ? RANK_RELATION_WITH_NODE
        : relation.hasTag("landuse", "residential") ? RANK_RESIDENTIAL_RELATION : RANK_PLAIN;
    record(relation, rank, relation.id());
  }

  private void record(WithTags feature, int rank, long id) {
    long[] candidate = { rank, id };
    for (String key : placeKeys(feature)) {
      bestByKey.merge(key, candidate, (current, cand) -> better(cand, current) ? cand : current);
    }
  }

  /**
   * Whether {@code candidate} outranks {@code current} (higher rank, then lower
   * id).
   */
  private static boolean better(long[] candidate, long[] current) {
    return candidate[0] > current[0] || (candidate[0] == current[0] && candidate[1] < current[1]);
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
  boolean isWinner(int rank, long id, Set<String> keys) {
    for (String key : keys) {
      long[] best = bestByKey.get(key);
      if (best != null && (best[0] != rank || best[1] != id)) {
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
      if (member.type() == OsmElement.Type.NODE) {
        double[] location = memberNodeLocations.get(member.ref());
        if (location != null) {
          return location;
        }
      }
    }
    return null;
  }

  /** How strongly this feature represents its place (see the class javadoc). */
  static int placeRank(SourceFeature feature) {
    if (!isPlace(feature) || feature.isPoint()) {
      return RANK_NODE;
    }
    boolean relation = feature instanceof OsmSourceFeature osm && osm.originalElement() instanceof OsmElement.Relation;
    if (relation && hasNodeMember(feature)) {
      return RANK_RELATION_WITH_NODE;
    }
    if (feature.hasTag("landuse", "residential")) {
      return relation ? RANK_RESIDENTIAL_RELATION : RANK_RESIDENTIAL_WAY;
    }
    return RANK_PLAIN;
  }

  /**
   * Whether this relation carries a node as a member (the settlement's anchor).
   */
  static boolean hasNodeMember(SourceFeature feature) {
    return feature instanceof OsmSourceFeature osm
        && osm.originalElement() instanceof OsmElement.Relation relation
        && relation.members().stream().anyMatch(member -> member.type() == OsmElement.Type.NODE);
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
