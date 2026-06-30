package il.org.osm.israelhiking;

import com.onthegomap.planetiler.reader.WithTags;

final class OsmFeatureClassifier {

  static final double DEFAULT_BASE_SCORE = 0.25;

  enum Category {
    NATURE_RESERVE("icon-leaf", "#008000", "Other", 0.80, 0.0),
    PROTECTED_NODE("icon-leaf", "#008000", "Other", DEFAULT_BASE_SCORE, 0.0),
    ROUTE_HIKING("icon-hike", "black", "Hiking", DEFAULT_BASE_SCORE, 0.0),
    ROUTE_BICYCLE("icon-bike", "black", "Bicycle", DEFAULT_BASE_SCORE, 0.0),
    ROUTE_4X4("icon-four-by-four", "black", "4x4", DEFAULT_BASE_SCORE, 0.0),
    HISTORIC_RUINS("icon-ruins", "#666666", "Historic", 0.55, 0.0),
    HISTORIC_ARCHAEOLOGICAL("icon-archaeological", "#666666", "Historic", 0.55, 0.0),
    HISTORIC_MEMORIAL("icon-memorial", "#666666", "Historic", 0.55, 0.0),
    HISTORIC_TOMB("icon-cave", "black", "Natural", 0.55, 0.0),
    PICNIC("icon-picnic", "#734a08", "Camping", DEFAULT_BASE_SCORE, 0.0),
    NATURAL_CAVE("icon-cave", "black", "Natural", 0.30, 0.0),
    NATURAL_SPRING("icon-tint", "#1e80e3", "Water", 0.30, 0.0),
    NATURAL_TREE("icon-tree", "#008000", "Natural", DEFAULT_BASE_SCORE, 0.0),
    NATURAL_FLOWERS("icon-flowers", "#008000", "Natural", DEFAULT_BASE_SCORE, 0.0),
    NATURAL_WATERHOLE("icon-waterhole", "#1e80e3", "Water", DEFAULT_BASE_SCORE, 0.0),
    PEAK("icon-peak", "black", "Natural", 0.30, 0.55),
    RIDGE("icon-peak", "black", "Natural", DEFAULT_BASE_SCORE, 0.0),
    WATER_BODY("icon-tint", "#1e80e3", "Water", DEFAULT_BASE_SCORE, 0.0),
    MAN_MADE_WATER_WELL("icon-water-well", "#1e80e3", "Water", DEFAULT_BASE_SCORE, 0.0),
    MAN_MADE_CISTERN("icon-cistern", "#1e80e3", "Water", DEFAULT_BASE_SCORE, 0.0),
    WATERFALL("icon-waterfall", "#1e80e3", "Water", 0.30, 0.0),
    WATERWAY_RELATION("icon-river", "#1e80e3", "Water", DEFAULT_BASE_SCORE, 0.0),
    PLACE_CITY("icon-home", "black", "Wikipedia", 1.00, 0.0),
    PLACE_TOWN("icon-home", "black", "Wikipedia", 0.80, 0.0),
    PLACE_VILLAGE("icon-home", "black", "Wikipedia", 0.55, 0.0),
    PLACE_HAMLET("icon-home", "black", "Wikipedia", 0.35, 0.0),
    PLACE_OTHER("icon-home", "black", "Wikipedia", 0.45, 0.0),
    PLACE_BLANK("icon-home", "black", "Wikipedia", DEFAULT_BASE_SCORE, 0.0),
    VIEWPOINT("icon-viewpoint", "#008000", "Viewpoint", 0.55, 0.0),
    CAMP_SITE("icon-campsite", "#734a08", "Camping", DEFAULT_BASE_SCORE, 0.0),
    ATTRACTION("icon-star", "#ffb800", "Other", 0.55, 0.0),
    ARTWORK("icon-artwork", "#ffb800", "Other", DEFAULT_BASE_SCORE, 0.0),
    ALPINE_HUT("icon-alpinehut", "#734a08", "Camping", 0.55, 0.0),
    HIGHWAY_CYCLEWAY("icon-bike", "black", "Bicycle", DEFAULT_BASE_SCORE, 0.0),
    HIGHWAY_FOOT("icon-hike", "black", "Hiking", DEFAULT_BASE_SCORE, 0.0),
    HIGHWAY_TRACK("icon-four-by-four", "black", "4x4", DEFAULT_BASE_SCORE, 0.0),
    WORSHIP_JEWISH("icon-synagogue", "black", "Other", DEFAULT_BASE_SCORE, 0.0),
    WORSHIP_CHRISTIAN("icon-church", "black", "Other", DEFAULT_BASE_SCORE, 0.0),
    WORSHIP_MUSLIM("icon-mosque", "black", "Other", DEFAULT_BASE_SCORE, 0.0),
    WORSHIP_OTHER("icon-holy-place", "black", "Other", DEFAULT_BASE_SCORE, 0.0),
    INATURE("icon-inature", "#116C00", "iNature", DEFAULT_BASE_SCORE, 0.0),
    FALLBACK_HISTORIC("icon-search", "black", "Other", 0.55, 0.0),
    FALLBACK_WATER("icon-search", "black", "Other", 0.30, 0.0),
    FALLBACK("icon-search", "black", "Other", DEFAULT_BASE_SCORE, 0.0),

    NONICON_GENERIC("icon-search", "black", "Other", DEFAULT_BASE_SCORE, 0.0),
    NONICON_STATION("icon-bus-stop", "black", "Other", DEFAULT_BASE_SCORE, 0.0),
    NONICON_RIDGE("icon-peak", "black", "Other", DEFAULT_BASE_SCORE, 0.0),
    NONICON_MTB("icon-bike", "green", "Bicycle", DEFAULT_BASE_SCORE, 0.0),
    NONICON_FOREST("icon-tree", "#008000", "Other", DEFAULT_BASE_SCORE, 0.0),
    NONICON_WIKIPEDIA("icon-wikipedia-w", "black", "Wikipedia", DEFAULT_BASE_SCORE, 0.0);

    final String icon;
    final String color;
    final String poiCategory;
    final double baseScore;
    final double elevationWeight;

    Category(String icon, String color, String poiCategory, double baseScore, double elevationWeight) {
      this.icon = icon;
      this.color = color;
      this.poiCategory = poiCategory;
      this.baseScore = baseScore;
      this.elevationWeight = elevationWeight;
    }
  }

  private OsmFeatureClassifier() {}

  static Category classify(WithTags f) {
    String boundary = f.getString("boundary");
    if ("protected_area".equals(boundary) || "national_park".equals(boundary)) {
      return Category.NATURE_RESERVE;
    }
    if ("nature_reserve".equals(f.getString("leisure"))) {
      return Category.PROTECTED_NODE;
    }

    String route = f.getString("route");
    if (route != null) {
      switch (route) {
        case "hiking":
        case "foot":
          return Category.ROUTE_HIKING;
        case "bicycle":
        case "mtb":
          return Category.ROUTE_BICYCLE;
        case "road":
          if ("yes".equals(f.getString("scenic"))) {
            return Category.ROUTE_4X4;
          }
      }
    }

    String historic = f.getString("historic");
    if (historic != null) {
      switch (historic) {
        case "ruins":
          return Category.HISTORIC_RUINS;
        case "archaeological_site":
          return Category.HISTORIC_ARCHAEOLOGICAL;
        case "memorial":
        case "monument":
          return Category.HISTORIC_MEMORIAL;
        case "tomb":
          return Category.HISTORIC_TOMB;
      }
    }

    if ("picnic_table".equals(f.getString("leisure"))
        || "picnic_site".equals(f.getString("tourism"))
        || "picnic".equals(f.getString("amenity"))) {
      return Category.PICNIC;
    }

    String natural = f.getString("natural");
    if (natural != null) {
      switch (natural) {
        case "cave_entrance":
          return Category.NATURAL_CAVE;
        case "spring":
          return Category.NATURAL_SPRING;
        case "tree":
          return Category.NATURAL_TREE;
        case "flowers":
          return Category.NATURAL_FLOWERS;
        case "waterhole":
          return Category.NATURAL_WATERHOLE;
        case "peak":
        case "volcano":
          return Category.PEAK;
        case "ridge":
          return Category.RIDGE;
      }
    }

    String water = f.getString("water");
    if ("reservoir".equals(water) || "pond".equals(water) || "lake".equals(water)
        || "stream_pool".equals(water)) {
      return Category.WATER_BODY;
    }

    String manMade = f.getString("man_made");
    if (manMade != null) {
      switch (manMade) {
        case "water_well":
          return Category.MAN_MADE_WATER_WELL;
        case "cistern":
          return Category.MAN_MADE_CISTERN;
      }
    }

    String waterway = f.getString("waterway");
    if ("waterfall".equals(waterway)) {
      return Category.WATERFALL;
    }

    if ("waterway".equals(f.getString("type"))) {
      return Category.WATERWAY_RELATION;
    }

    String place = f.getString("place");
    if (place != null) {
      if (place.isBlank()) {
        return Category.PLACE_BLANK;
      }
      switch (place) {
        case "city":
          return Category.PLACE_CITY;
        case "town":
          return Category.PLACE_TOWN;
        case "village":
          return Category.PLACE_VILLAGE;
        case "hamlet":
          return Category.PLACE_HAMLET;
        default:
          return Category.PLACE_OTHER;
      }
    }

    String tourism = f.getString("tourism");
    if (tourism != null) {
      switch (tourism) {
        case "viewpoint":
          return Category.VIEWPOINT;
        case "camp_site":
          return Category.CAMP_SITE;
        case "attraction":
          return Category.ATTRACTION;
        case "artwork":
          return Category.ARTWORK;
        case "alpine_hut":
          return Category.ALPINE_HUT;
      }
    }

    String highway = f.getString("highway");
    if (highway != null) {
      switch (highway) {
        case "cycleway":
          return Category.HIGHWAY_CYCLEWAY;
        case "footway":
        case "path":
          return Category.HIGHWAY_FOOT;
        case "track":
          return Category.HIGHWAY_TRACK;
      }
    }

    String amenity = f.getString("amenity");
    if ("place_of_worship".equals(amenity) || "monastery".equals(amenity)) {
      String religion = f.getString("religion") != null ? f.getString("religion") : "";
      switch (religion) {
        case "jewish":
          return Category.WORSHIP_JEWISH;
        case "christian":
          return Category.WORSHIP_CHRISTIAN;
        case "muslim":
          return Category.WORSHIP_MUSLIM;
        default:
          return Category.WORSHIP_OTHER;
      }
    }

    if (f.getString("ref:IL:inature") != null) {
      return Category.INATURE;
    }

    if (historic != null) {
      return Category.FALLBACK_HISTORIC;
    }
    if ("hot_spring".equals(natural) || waterway != null) {
      return Category.FALLBACK_WATER;
    }
    return Category.FALLBACK;
  }

  static Category classifyNonIcon(WithTags f) {
    Category c = null;
    if (f.hasTag("amenity", "place_of_worship") || f.hasTag("natural", "valley")) {
      c = Category.NONICON_GENERIC;
    }
    if (f.hasTag("building") && !f.hasTag("building", "no", "none", "No")) {
      c = Category.NONICON_GENERIC;
    }
    if (f.hasTag("railway", "station") || f.hasTag("aerialway", "station")) {
      c = Category.NONICON_STATION;
    }
    if (f.hasTag("natural", "ridge")) {
      c = Category.NONICON_RIDGE;
    }
    if (f.hasTag("landuse", "recreation_ground") && f.hasTag("sport", "mtb")) {
      c = Category.NONICON_MTB;
    }
    if (f.hasTag("landuse", "forest")) {
      c = Category.NONICON_FOREST;
    }
    if (c == null) {
      return null;
    }
    if (c == Category.NONICON_GENERIC
        && (f.getString("wikidata") != null || f.getString("wikipedia") != null)) {
      return Category.NONICON_WIKIPEDIA;
    }
    return c;
  }
}
