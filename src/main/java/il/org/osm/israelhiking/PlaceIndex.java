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
 * forms seen in the first pass, so the second pass can index every place exactly
 * once under the ranking <b>relation &gt; node &gt; way</b>:
 * <ul>
 * <li>a place is searchable by name even when it has no dedicated place node
 * (common in Israel);</li>
 * <li>a place with several representations shows up only once;</li>
 * <li>when a relation wins it inherits the node's tags, so identity and ranking
 * fields such as {@code wikidata} and {@code population} are not lost.</li>
 * </ul>
 *
 * Populated from {@code preprocessOsm*} on pass 1 and queried from
 * {@code processFeature} on pass 2; both run multi-threaded, so the backing map
 * and the per-place fields are concurrency-safe.
 */
final class PlaceIndex {

  /** Which OSM form a second-pass feature came from. */
  enum Kind {
    NODE, WAY, RELATION
  }

  /**
   * The node tags worth carrying onto a winning relation: the identity, ranking
   * and metadata fields a boundary relation usually lacks. Every other tag is
   * dropped so this index stays small on a planet-wide build; name and
   * description tags are kept separately via {@link #isNameOrDescriptionTag}.
   */
  private static final Set<String> MERGE_TAG_KEYS = Set.of(
      "name", "description", "wikidata", "image", "wikimedia_commons", "website", "ele", "population");

  private final Map<String, PlaceInfo> byKey = new ConcurrentHashMap<>();

  /** The first-pass knowledge about one place. Fields are written from many threads. */
  private static final class PlaceInfo {
    /** Trimmed tags of the place node, or null when the place has no node. */
    volatile Map<String, Object> nodeTags;
    /** Whether a place relation that resolves to a polygon exists. */
    volatile boolean hasRelation;
  }

  // ---- first pass ----

  /** Records a named place node so a same-named polygon can defer to it and inherit its tags. */
  void recordNode(OsmElement.Node node) {
    if (!node.hasTag("place") || !node.hasTag("name")) {
      return;
    }
    var nodeTags = trimPlaceTags(node.tags());
    for (String key : placeKeys(node)) {
      byKey.computeIfAbsent(key, k -> new PlaceInfo()).nodeTags = nodeTags;
    }
  }

  /**
   * Records a place relation that {@link #resolvesToPolygon resolves to a
   * polygon}, so its node and member ways can defer to it in the second pass.
   */
  void recordRelation(OsmElement.Relation relation) {
    if (!relation.hasTag("place") || !resolvesToPolygon(relation)) {
      return;
    }
    for (String key : placeKeys(relation)) {
      byKey.computeIfAbsent(key, k -> new PlaceInfo()).hasRelation = true;
    }
  }

  // ---- second pass ----

  /**
   * The tags to index for this place representation, or {@code null} when
   * another representation should carry it. A winning relation inherits the
   * node's trimmed tags (the relation's own tags win on conflict, keeping its
   * geometry-derived identity); a winning node or way is returned unchanged.
   */
  WithTags winningTags(Kind kind, WithTags feature) {
    PlaceInfo info = lookup(feature);
    boolean hasRelation = info != null && info.hasRelation;
    Map<String, Object> nodeTags = info == null ? null : info.nodeTags;
    switch (kind) {
      case NODE:
        // A node yields to a relation of the same place, but outranks a way.
        return hasRelation ? null : feature;
      case WAY:
        // A way is used only when neither a node nor a relation represents the place.
        return (nodeTags != null || hasRelation) ? null : feature;
      case RELATION:
      default:
        if (nodeTags == null) {
          return feature;
        }
        var merged = new HashMap<>(nodeTags);
        merged.putAll(feature.tags());
        return WithTags.from(merged);
    }
  }

  /** Which OSM form produced this second-pass feature. */
  static Kind kindOf(SourceFeature feature) {
    if (feature.isPoint()) {
      return Kind.NODE;
    }
    if (feature instanceof OsmSourceFeature osm && osm.originalElement() instanceof OsmElement.Relation) {
      return Kind.RELATION;
    }
    return Kind.WAY;
  }

  /** Whether the feature has any name we can search on, in the default or a supported language. */
  static boolean hasSearchableName(WithTags feature, String[] languages) {
    if (feature.hasTag("name")) {
      return true;
    }
    for (String language : languages) {
      if (feature.hasTag("name:" + language)) {
        return true;
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

  // ---- key derivation and trimming (package-private for unit tests) ----

  /**
   * Whether planetiler will turn this relation into a polygon in the second
   * pass, mirroring its own multipolygon test. Only such a relation is worth
   * deferring to: recording one that never materializes would suppress the node
   * that still represents the place and drop it from search entirely.
   */
  static boolean resolvesToPolygon(OsmElement.Relation relation) {
    return relation.hasTag("type", "multipolygon", "boundary", "land_area")
        && relation.members().stream().anyMatch(member -> member.type() == OsmElement.Type.WAY);
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

  /** Keeps only the node tags a winning relation should inherit (see {@link #MERGE_TAG_KEYS}). */
  static Map<String, Object> trimPlaceTags(Map<String, Object> tags) {
    var trimmed = new HashMap<String, Object>();
    tags.forEach((key, value) -> {
      if (MERGE_TAG_KEYS.contains(key) || isNameOrDescriptionTag(key)) {
        trimmed.put(key, value);
      }
    });
    return trimmed;
  }

  private static boolean isNameOrDescriptionTag(String key) {
    if (key.startsWith("name:") || key.startsWith("description:")) {
      return true;
    }
    for (String alt : PlanetSearchProfile.ALTERNATIVE_NAME_TAGS) {
      if (key.equals(alt) || key.startsWith(alt + ":")) {
        return true;
      }
    }
    return false;
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
