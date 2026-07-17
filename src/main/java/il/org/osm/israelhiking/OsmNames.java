package il.org.osm.israelhiking;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.onthegomap.planetiler.reader.WithTags;

/** Helpers for reasoning about a feature's OSM name tags. */
final class OsmNames {

  private static final List<String> ALTERNATIVE_NAME_TAGS = List.of(
      "alt_name", "loc_name", "short_name", "old_name", "official_name");

  private OsmNames() {
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
   * The alternative names of a feature for one language, read from tags such as
   * {@code alt_name} / {@code loc_name}. The "default" language reads the
   * unsuffixed tags; each tag may hold several {@code ;}-separated names.
   */
  static List<String> alternativeNames(WithTags feature, String language) {
    var suffix = "default".equals(language) ? "" : ":" + language;
    return ALTERNATIVE_NAME_TAGS.stream()
        .map(tag -> feature.getString(tag + suffix))
        .filter(Objects::nonNull)
        .flatMap(value -> Arrays.stream(value.split(";")))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .distinct()
        .toList();
  }

  /** Whether the tag key holds a name or description in any language (including the alt-name tags). */
  static boolean isNameOrDescriptionTag(String key) {
    if (key.startsWith("name:") || key.startsWith("description:")) {
      return true;
    }
    for (String alt : ALTERNATIVE_NAME_TAGS) {
      if (key.equals(alt) || key.startsWith(alt + ":")) {
        return true;
      }
    }
    return false;
  }
}
