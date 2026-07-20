package il.org.osm.israelhiking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmSourceFeature;

/**
 * Tracks how each place is represented across the OSM node / way / relation
 * element seen in the first pass, so the second pass can index every place
 * exactly
 * once under the ranking relation then node then way:
 * - a place is searchable by name even when it has no dedicated place node
 * (common in Israel);
 * - a place with several representations shows up only once;
 * - when a relation wins it inherits the node's tags, so identity and ranking
 * fields such as {@code wikidata} and {@code population} are not lost.
 *
 * Populated from {@code preprocessOsm*} on pass 1 and queried from
 * {@code processFeature} on pass 2; both run multi-threaded, so the backing map
 * and the per-place fields are concurrency-safe.
 */
final class PlaceIndex {

  /** Which OSM element type a second-pass feature came from. */
  enum Kind {
    NODE, WAY, RELATION
  }

  /**
   * The node tags worth carrying onto a winning relation: the identity, ranking
   * and metadata fields a boundary relation usually lacks. Every other tag is
   * dropped so this index stays small on a planet-wide build; name and
   * description tags are kept separately via {@link OsmNames#isNameOrDescriptionTag}.
   */
  private static final Set<String> MERGE_TAG_KEYS = Set.of(
      "name", "description", "wikidata", "image", "wikimedia_commons", "website", "ele", "population");

  private final Map<String, PlaceInfo> byKey = new ConcurrentHashMap<>();

  /**
   * The first-pass knowledge about one place. Fields are written from many
   * threads.
   */
  private static final class PlaceInfo {
    /** Trimmed tags of the place node, or null when the place has no node. */
    volatile Map<String, Object> nodeTags;
    /** Whether a place relation that resolves to a polygon exists. */
    volatile boolean hasRelation;
  }

  /**
   * Records a named place node so a same-named polygon can defer to it and
   * inherit its tags.
   */
  void recordNode(OsmElement.Node node, String[] languages) {
    if (!node.hasTag("place") || !OsmNames.hasSearchableName(node, languages)) {
      return;
    }
    var nodeTags = trimPlaceTags(node.tags());
    for (String key : placeKeys(node)) {
      byKey.computeIfAbsent(key, k -> new PlaceInfo()).nodeTags = nodeTags;
    }
  }

  /**
   * Records a place relation, but only when planetiler will turn it into a
   * polygon in the second pass — a polygonal type with a way member, mirroring
   * its own multipolygon test. Recording one that never materializes would
   * suppress the node that still represents the place and drop it from search.
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
    for (String key : placeKeys(relation)) {
      byKey.computeIfAbsent(key, k -> new PlaceInfo()).hasRelation = true;
    }
  }

  /**
   * Whether this representation is the one to index for its place, applying the
   * ranking relation &gt; node &gt; way: a node yields to a relation of the same
   * place but outranks a way, a way is used only when neither a node nor a
   * relation represents the place, and a relation always wins.
   */
  boolean shouldIndex(Kind kind, WithTags feature) {
    PlaceInfo info = lookup(feature);
    boolean hasRelation = info != null && info.hasRelation;
    boolean hasNode = info != null && info.nodeTags != null;
    return switch (kind) {
      case NODE -> !hasRelation;
      case WAY -> !hasNode && !hasRelation;
      case RELATION -> true;
    };
  }

  /**
   * The tags to index for a winning representation: a relation inherits the
   * matching node's trimmed tags (its own tags win on conflict, keeping its
   * geometry-derived identity), while a node or way is used unchanged.
   */
  WithTags tagsToIndex(Kind kind, WithTags feature) {
    if (kind != Kind.RELATION) {
      return feature;
    }
    PlaceInfo info = lookup(feature);
    if (info == null || info.nodeTags == null) {
      return feature;
    }
    var merged = new HashMap<>(info.nodeTags);
    merged.putAll(feature.tags());
    return WithTags.from(merged);
  }

  /** Which OSM element type produced this second-pass feature. */
  static Kind kindOf(SourceFeature feature) {
    if (feature.isPoint()) {
      return Kind.NODE;
    }
    if (feature instanceof OsmSourceFeature osm && osm.originalElement() instanceof OsmElement.Relation) {
      return Kind.RELATION;
    }
    return Kind.WAY;
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

  /**
   * Keeps only the node tags a winning relation should inherit (see
   * {@link #MERGE_TAG_KEYS}).
   */
  static Map<String, Object> trimPlaceTags(Map<String, Object> tags) {
    var trimmed = new HashMap<String, Object>();
    tags.forEach((key, value) -> {
      if (MERGE_TAG_KEYS.contains(key) || OsmNames.isNameOrDescriptionTag(key)) {
        trimmed.put(key, value);
      }
    });
    return trimmed;
  }

  /**
   * What the first pass learned about the place this feature belongs to, or
   * null. When the name and wikidata keys point at different places (a node and
   * relation that agree on wikidata but not name, say), the two are combined so
   * neither the node's tags nor the relation's existence is missed.
   */
  private PlaceInfo lookup(WithTags feature) {
    PlaceInfo byName = feature.hasTag("name") ? byKey.get("name=" + feature.getString("name")) : null;
    var wikidata = feature.getString("wikidata");
    PlaceInfo byWikidata = wikidata != null ? byKey.get("wikidata=" + wikidata) : null;
    if (byName == null) {
      return byWikidata;
    }
    if (byWikidata == null || byWikidata == byName) {
      return byName;
    }
    var combined = new PlaceInfo();
    combined.nodeTags = byName.nodeTags != null ? byName.nodeTags : byWikidata.nodeTags;
    combined.hasRelation = byName.hasRelation || byWikidata.hasRelation;
    return combined;
  }
}
