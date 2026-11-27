package il.org.osm.israelhiking;

import java.util.Map;

import com.onthegomap.planetiler.reader.WithTags;

/** The color a route is drawn in on the routes layer. */
final class RouteColor {

  private RouteColor() {
  }

  /**
   * The route's {@code colour}, or else the way color that opens its
   * {@code osmc:symbol}, which is what hiking maps paint the trail in.
   * Otherwise a color whose hue is hashed from its name — the unsuffixed one,
   * which does not change with the build's languages, or its id when it has no
   * name — so it keeps its color between builds. {@link String#hashCode} is
   * specified, and is mixed so that similar names do not get similar hues.
   *
   * @param names the route's names per language, as on its point document
   */
  static String of(WithTags route, Map<String, String> names, long id) {
    var colour = route.getString("colour");
    if (colour != null && !colour.isBlank()) {
      return colour.trim();
    }
    var wayColor = route.getString("osmc:symbol", "").split(":", 2)[0].trim();
    if (!wayColor.isEmpty()) {
      return wayColor;
    }
    var key = names.getOrDefault("default",
        names.values().stream().sorted().findFirst().orElse(String.valueOf(id)));
    int h = key.hashCode();
    h ^= h >>> 16;
    h *= 0x45d9f3b;
    h ^= h >>> 16;
    var rgb = java.awt.Color.HSBtoRGB(Math.floorMod(h, 360) / 360f, 0.8f, 0.75f);
    return String.format("#%06x", rgb & 0xffffff);
  }
}
