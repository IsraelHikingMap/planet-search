package il.org.osm.israelhiking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

final class OsmTagUtils {

  private OsmTagUtils() {
  }

  static String classifyFeatureClass(Function<String, String> tagLookup) {
    String natural = tagLookup.apply("natural");
    String waterway = tagLookup.apply("waterway");
    String place = tagLookup.apply("place");
    String historic = tagLookup.apply("historic");
    String tourism = tagLookup.apply("tourism");
    String leisure = tagLookup.apply("leisure");
    String amenity = tagLookup.apply("amenity");
    String shop = tagLookup.apply("shop");
    String office = tagLookup.apply("office");
    String craft = tagLookup.apply("craft");
    String healthcare = tagLookup.apply("healthcare");
    String manMade = tagLookup.apply("man_made");
    String building = tagLookup.apply("building");

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
        case "water":                 fc = "lake"; break;
        case "spring": case "hot_spring": fc = "spring"; break;
        case "glacier":               fc = "glacier"; break;
        case "bay":                   fc = "bay"; break;
        case "cape":                  fc = "cape"; break;
        case "beach":                 fc = "beach"; break;
        case "wood": case "forest":   fc = "forest"; break;
        case "valley":                fc = "valley"; break;
        case "canyon": case "gorge":  fc = "canyon"; break;
        case "mesa": case "plateau":  fc = "plateau"; break;
        case "arch":                  fc = "arch"; break;
        case "cave_entrance":         fc = "cave"; break;
        case "wetland":               fc = "wetland"; break;
        case "arete":                 fc = "ridge"; break;
        case "crater":                fc = "peak"; break;
        case "mountain_range":        fc = "peak"; break;
        default:                      fc = "natural"; break;
      }
      String water = tagLookup.apply("water");
      if ("water".equals(natural) && water != null) {
        switch (water) {
          case "lake":      fc = "lake"; break;
          case "reservoir": fc = "reservoir"; break;
          case "pond":      fc = "pond"; break;
          case "river":     fc = "river"; break;
          case "canal":     fc = "canal"; break;
          case "lagoon":    fc = "lagoon"; break;
          default:          fc = "water"; break;
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
        case "hotel": case "motel": case "hostel": case "guest_house":
        case "apartment": case "chalet": case "alpine_hut": case "wilderness_hut":
                                      fc = "lodging"; break;
        case "museum": case "gallery": fc = "museum"; break;
        case "information":           fc = "tourism"; break;
        default:                      fc = "tourism"; break;
      }
    } else if (leisure != null) {
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

  private static final String[] ALT_NAME_TAG_BASES = {
      "alt_name", "official_name", "short_name", "loc_name", "int_name"
  };

  /** old_name is deliberately excluded — a stale former name can overtake the real one. */
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
    var collectedDefault = collectVariants(tagLookup, ALT_NAME_TAG_BASES);
    if (collectedDefault != null) {
      if (altNames == null) {
        altNames = new HashMap<String, List<String>>();
      }
      altNames.put("default", collectedDefault);
    }
    return altNames;
  }

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

  // Lowercase 'm' excluded: it's metres, so "1500m" parses to 1500, not mega.
  private static final String GLUED_MAGNITUDE_SUFFIXES = "kKMB";
  private static final double[] GLUED_MAGNITUDE_VALUES = { 1e3, 1e3, 1e6, 1e9 };

  static double parseFirstNumber(String raw) {
    if (raw == null) {
      return Double.NaN;
    }
    StringBuilder sb = new StringBuilder();
    boolean seenDigit = false;
    boolean seenDot = false;
    boolean seenSign = false;
    boolean lastWasNumeric = false;
    double multiplier = 1.0;
    for (int i = 0; i < raw.length(); i++) {
      char c = raw.charAt(i);
      if (c >= '0' && c <= '9') {
        sb.append(c);
        seenDigit = true;
        lastWasNumeric = true;
      } else if (c == '-' && !seenDigit && !seenSign) {
        sb.append(c);
        seenSign = true;
      } else if (c == '.' && seenDigit && !seenDot) {
        sb.append(c);
        seenDot = true;
        lastWasNumeric = true;
      } else if ((c == ',' || c == ' ' || c == '\'') && seenDigit) {
        lastWasNumeric = false;
      } else if (seenDigit) {
        int magIdx = lastWasNumeric ? GLUED_MAGNITUDE_SUFFIXES.indexOf(c) : -1;
        if (magIdx >= 0) {
          multiplier = GLUED_MAGNITUDE_VALUES[magIdx];
        }
        break;
      }
    }
    if (!seenDigit) {
      return Double.NaN;
    }
    try {
      return Double.parseDouble(sb.toString()) * multiplier;
    } catch (NumberFormatException e) {
      return Double.NaN;
    }
  }

  /** isFinite rejects a +Infinity overflow that would otherwise pin to MAX_VALUE; null lets ES use missing:1.0. */
  static Integer computePopulation(String place, String populationTag) {
    if (place == null) {
      return null;
    }
    double parsed = parseFirstNumber(populationTag);
    if (Double.isFinite(parsed) && parsed >= 1) {
      return (int) Math.min(parsed, Integer.MAX_VALUE);
    }
    switch (place) {
      case "city":    return 1_000_000;
      case "town":    return 50_000;
      case "village": return 2_000;
      case "hamlet":  return 200;
      default:        return null;
    }
  }
}
