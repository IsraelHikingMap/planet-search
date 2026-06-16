package il.org.osm.israelhiking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Pure (no I/O, no instance state) OSM-tag/string helpers extracted from PlanetSearchProfile so the
 * parsing/classification logic is independently unit-testable. Every method takes its inputs
 * explicitly (a tag-lookup function or a raw string) and returns a value — none touch profile state.
 */
final class OsmTagUtils {

  private OsmTagUtils() {
    // utility class — not instantiable
  }

  /**
   * Reads the OSM tags that both classifyFeatureClass and the icon classifier need, once, into final
   * fields so neither has to re-read them. getString is a pure null-on-absent lookup, so caching the
   * value is identical to calling it each time. (protect_class/protection_title stay direct reads in
   * setProtectedAreaIcon — only that branch uses them.)
   */
  static final class OsmTagCache {
    final String natural, waterway, place, historic, tourism, leisure, amenity, shop, office, craft,
        healthcare, manMade, building, water, boundary, route, scenic, type, highway, religion,
        refILInature;

    OsmTagCache(Function<String, String> tagLookup) {
      this.natural = tagLookup.apply("natural");
      this.waterway = tagLookup.apply("waterway");
      this.place = tagLookup.apply("place");
      this.historic = tagLookup.apply("historic");
      this.tourism = tagLookup.apply("tourism");
      this.leisure = tagLookup.apply("leisure");
      this.amenity = tagLookup.apply("amenity");
      this.shop = tagLookup.apply("shop");
      this.office = tagLookup.apply("office");
      this.craft = tagLookup.apply("craft");
      this.healthcare = tagLookup.apply("healthcare");
      this.manMade = tagLookup.apply("man_made");
      this.building = tagLookup.apply("building");
      this.water = tagLookup.apply("water");
      this.boundary = tagLookup.apply("boundary");
      this.route = tagLookup.apply("route");
      this.scenic = tagLookup.apply("scenic");
      this.type = tagLookup.apply("type");
      this.highway = tagLookup.apply("highway");
      this.religion = tagLookup.apply("religion");
      this.refILInature = tagLookup.apply("ref:IL:inature");
    }
  }

  // Language-neutral OSM variant-name tag bases (no :lang suffix); also have per-language forms
  // (alt_name:he, ...). old_name excluded — a stale former name can overtake the real one.
  private static final String[] ALT_NAME_TAG_BASES = {
      "alt_name", "official_name", "short_name", "loc_name", "int_name"
  };

  /**
   * Pure (no I/O) builder for the alt_names map, so the ";"-split, trim, de-dup and old_name
   * exclusion are unit-testable. Returns null when no variant tags are present.
   *
   * @param supportedLanguages the per-language keys to look up suffixed tags for
   * @param tagLookup          maps an OSM tag key to its raw value (null if absent)
   */
  static Map<String, List<String>> buildAltNames(String[] supportedLanguages,
      Function<String, String> tagLookup) {
    Map<String, List<String>> altNames = null;
    for (String language : supportedLanguages) {
      var suffixedTags = Arrays.stream(ALT_NAME_TAG_BASES)
          .map(base -> base + ":" + language)
          .toArray(String[]::new);
      var collected = collectVariants(tagLookup, suffixedTags);
      if (collected != null) {
        if (altNames == null) {
          altNames = new HashMap<String, List<String>>();
        }
        altNames.put(language, collected);
      }
    }
    // Bare (language-neutral) tags go under "default".
    var collectedDefault = collectVariants(tagLookup, ALT_NAME_TAG_BASES);
    if (collectedDefault != null) {
      if (altNames == null) {
        altNames = new HashMap<String, List<String>>();
      }
      altNames.put("default", collectedDefault);
    }
    return altNames;
  }

  /**
   * Read each of tags, split every value on ";", trim, drop empties, de-dup (insertion order kept).
   * Returns null if nothing survives. old_name is never in tags.
   */
  private static List<String> collectVariants(Function<String, String> tagLookup, String[] tags) {
    var variants = new LinkedHashSet<String>();
    for (String tag : tags) {
      var raw = tagLookup.apply(tag);
      if (raw == null || raw.isEmpty()) {
        continue;
      }
      for (String part : raw.split(";")) {
        var trimmed = part.trim();
        if (!trimmed.isEmpty()) {
          variants.add(trimmed);
        }
      }
    }
    return variants.isEmpty() ? null : new ArrayList<>(variants);
  }

  /**
   * Pure core of setFeatureClass, so the OSM-tag -> feature_class switch is unit-testable from a
   * literal tag map. Returns null when nothing recognised is present.
   */
  static String classifyFeatureClass(Function<String, String> tagLookup) {
    return classifyFeatureClass(new OsmTagCache(tagLookup));
  }

  static String classifyFeatureClass(OsmTagCache tags) {
    String natural = tags.natural;
    String waterway = tags.waterway;
    String place = tags.place;
    String historic = tags.historic;
    String tourism = tags.tourism;
    // Built/POI keys: consulted only after the outdoor keys above, so an object carrying both an
    // outdoor tag and a secondary built tag keeps its outdoor class. Unnamed objects never reach
    // here (every emit path gates on name), so we classify only named features.
    String leisure = tags.leisure;
    String amenity = tags.amenity;
    String shop = tags.shop;
    String office = tags.office;
    String craft = tags.craft;
    String healthcare = tags.healthcare;
    String manMade = tags.manMade;
    String building = tags.building;

    String fc = null;
    if (natural != null) {
      switch (natural) {
        case "peak":                  fc = "peak"; break;
        case "volcano":               fc = "peak"; break;
        case "hill":                  fc = "hill"; break;
        case "ridge":                 fc = "ridge"; break;
        case "saddle": case "gap":    fc = "saddle"; break;
        case "cliff":                 fc = "cliff"; break;
        case "rock": case "stone":    fc = "rock"; break;
        case "water":                 fc = "lake"; break;   // refined below by water=*
        case "spring": case "hot_spring": fc = "spring"; break;
        case "glacier":               fc = "glacier"; break;
        case "bay":                   fc = "bay"; break;
        case "cape":                  fc = "cape"; break;
        case "beach":                 fc = "beach"; break;
        case "wood": case "forest":   fc = "forest"; break;
        case "valley":                fc = "valley"; break;
        // landforms scoring already groups (TERRAIN_RELIEF) but the indexer never emitted:
        case "canyon": case "gorge":  fc = "canyon"; break;
        case "mesa": case "plateau":  fc = "plateau"; break;
        case "arch":                  fc = "arch"; break;
        case "cave_entrance":         fc = "cave"; break;
        case "wetland":               fc = "wetland"; break;
        case "arete":                 fc = "ridge"; break;    // fold: ridge kin
        case "crater":                fc = "peak"; break;     // fold: volcanic-summit kin
        case "mountain_range":        fc = "peak"; break;     // fold: high-ground kin
        default:                      fc = "natural"; break;
      }
      // refine natural=water by its sub-type (lake vs reservoir vs pond)
      String water = tags.water;
      if ("water".equals(natural) && water != null) {
        switch (water) {
          case "lake":      fc = "lake"; break;
          case "reservoir": fc = "reservoir"; break;
          case "pond":      fc = "pond"; break;
          default:          fc = "lake"; break;
        }
      }
    } else if (waterway != null) {
      switch (waterway) {
        case "waterfall":             fc = "waterfall"; break;
        case "river":                 fc = "river"; break;
        case "stream":                fc = "stream"; break;
        case "canal":                 fc = "canal"; break;
        case "rapids":                fc = "rapids"; break;
        default:                      fc = "waterway"; break;
      }
    } else if (place != null) {
      switch (place) {
        case "city": case "town": case "village": case "hamlet":
        case "suburb": case "neighbourhood":
                                      fc = place; break;
        case "island": case "islet": fc = "island"; break;
        case "locality":             fc = "locality"; break;
        default:                      fc = "place"; break;
      }
    } else if (historic != null) {
      fc = "historic";
    } else if (tourism != null) {
      switch (tourism) {
        case "viewpoint":             fc = "viewpoint"; break;
        case "camp_site":             fc = "campsite"; break;
        case "attraction":            fc = "attraction"; break;
        // lodging (built): folded into one "lodging" class
        case "hotel": case "motel": case "hostel": case "guest_house":
        case "apartment": case "chalet": case "alpine_hut": case "wilderness_hut":
                                      fc = "lodging"; break;
        case "museum": case "gallery": fc = "museum"; break;
        case "information":           fc = "tourism"; break;
        default:                      fc = "tourism"; break;
      }
    } else if (leisure != null) {
      // Recreation: park/nature_reserve lean OUTDOOR (semi-outdoor group), sports the rest.
      switch (leisure) {
        case "park": case "garden": case "nature_reserve":
        case "playground": case "dog_park": case "common":
                                      fc = "park"; break;
        case "sports_centre": case "stadium": case "pitch": case "track":
        case "fitness_centre": case "swimming_pool": case "golf_course":
        case "ice_rink": case "horse_riding":
                                      fc = "sports"; break;
        default:                      fc = "leisure"; break;
      }
    } else if (amenity != null) {
      switch (amenity) {
        case "restaurant": case "cafe": case "fast_food": case "bar":
        case "pub": case "food_court": case "biergarten": case "ice_cream":
                                      fc = "food"; break;
        case "place_of_worship": case "monastery":
                                      fc = "religious"; break;
        case "school": case "university": case "college": case "kindergarten":
        case "library":
                                      fc = "education"; break;
        case "hospital": case "clinic": case "pharmacy": case "doctors":
        case "dentist": case "veterinary":
                                      fc = "medical"; break;
        case "townhall": case "courthouse": case "police": case "fire_station":
        case "post_office": case "embassy": case "prison":
                                      fc = "government"; break;
        case "fuel": case "charging_station":
                                      fc = "fuel"; break;
        case "parking": case "parking_space": case "bicycle_parking":
        case "motorcycle_parking": case "taxi":
                                      fc = "parking"; break;
        case "bus_station": case "ferry_terminal":
                                      fc = "transit"; break;
        case "theatre": case "cinema": case "arts_centre":
                                      fc = "museum"; break;
        default:                      fc = "amenity"; break;
      }
    } else if (shop != null) {
      fc = "shop";
    } else if (office != null) {
      fc = "office";
    } else if (craft != null) {
      fc = "office";
    } else if (healthcare != null) {
      fc = "medical";
    } else if (manMade != null) {
      switch (manMade) {
        case "tower": case "lighthouse": case "bridge": case "obelisk":
        case "water_tower": case "windmill": case "watermill": case "pier":
                                      fc = "structure"; break;
        default:                      fc = "man_made"; break;
      }
    } else if (building != null && !"no".equals(building) && !"none".equals(building)
               && !"No".equals(building)) {
      // Named building with no more-specific type tag above. building=yes is the dominant
      // (~80%) value; a named one is a real search target (a named hall/landmark), so it gets
      // the generic "building" class rather than null.
      switch (building) {
        case "church": case "chapel": case "cathedral": case "mosque":
        case "synagogue": case "temple": case "shrine":
                                      fc = "religious"; break;
        case "hotel":                 fc = "lodging"; break;
        case "hospital":              fc = "medical"; break;
        case "school": case "university": case "college":
                                      fc = "education"; break;
        case "train_station":         fc = "transit"; break;
        default:                      fc = "building"; break;
      }
    }
    return fc;
  }

  /**
   * Parse the first number out of a free-text OSM value like "4302", "14,115 ft", "1 000", "-413",
   * "yes". A leading '-' is honored (below-sea-level elevations are real — e.g. the Dead Sea region),
   * but a '-' AFTER digits ends the number ("100-200" -> 100). Returns Double.NaN when there is no
   * usable number. Never throws.
   */
  static double parseFirstNumber(String raw) {
    if (raw == null) {
      return Double.NaN;
    }
    StringBuilder sb = new StringBuilder();
    boolean seenDigit = false;
    boolean seenDot = false;
    boolean seenSign = false;
    for (int i = 0; i < raw.length(); i++) {
      char c = raw.charAt(i);
      if (c >= '0' && c <= '9') {
        sb.append(c);
        seenDigit = true;
      } else if (c == '-' && !seenDigit && !seenSign) {
        // leading minus only (before any digit) — a negative value such as a below-sea-level ele
        sb.append(c);
        seenSign = true;
      } else if (c == '.' && seenDigit && !seenDot) {
        sb.append(c);
        seenDot = true;
      } else if ((c == ',' || c == ' ' || c == '\'') && seenDigit) {
        // thousands separator within a number — skip it
      } else if (seenDigit) {
        break; // number ended (e.g. " ft", a trailing "-" range separator)
      }
    }
    if (!seenDigit) {
      return Double.NaN;
    }
    try {
      return Double.parseDouble(sb.toString());
    } catch (NumberFormatException e) {
      return Double.NaN;
    }
  }
}
