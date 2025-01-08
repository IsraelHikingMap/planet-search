package com.example;

import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.WAY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.operation.linemerge.LineMerger;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;

public class GlobalSearchProfile implements Profile {
  private PlanetilerConfig config;

  public GlobalSearchProfile(PlanetilerConfig config) {
    this.config = config;
  }

  /*
   * The processing happens in 3 steps:
   * 1. On the first pass through the input file, store relevant information from applicable OSM route relations and ways with mtb:name tag.
   * 2. On the second pass, emit points for relation and mtb:name ways. Emit a point by merging all the ways and using the first point of the merged linestring.
   */

  /*
   * Step 1)
   *
   * Planetiler processes the .osm.pbf input file in two passes. The first pass stores node locations, and invokes
   * preprocessOsmRelation for reach relation and stores information the profile needs during the second pass when we
   * emit map feature for ways contained in that relation.
   */

  // Minimal container for data we extract from OSM route relations. This is held in RAM so keep it small.
  private record RouteRelationInfo(
    // OSM ID of the relation (required):
    @Override long id,
    // Values for tags extracted from the OSM relation:
    String name,
    String name_he,
    String name_en,
    String route, // a.k.a. class
    String network,
    // For route segments only
    String ref,
    String osmc_symbol,
    String colour,
    // For POIs only
    String description,
    String description_he,
    String description_en,
    String wikidata,
    String image,
    String wikimedia_commons,
    String route_type,
    Long first_member_id,
    List<Long> member_ids
  ) implements OsmRelationInfo {}

  private class SinglesIds {
    List<Long> ids;
    long minId;
    SinglesIds(List<Long> ids, long minId) {
      this.ids = ids;
      this.minId = minId;
    }
  }

  private class MergedLinesHelper {
    LineMerger lineMerger;
    SourceFeature feature;
    MergedLinesHelper() {
      this.lineMerger = new LineMerger();
    }
  }

  static private final Map<String, SinglesIds> Singles = new HashMap<>();
  static private final ConcurrentMap<Long, MergedLinesHelper> RelationLineMergers = new ConcurrentHashMap<>();
  static private final ConcurrentMap<Long, MergedLinesHelper> SinglesLineMergers = new ConcurrentHashMap<>();

  static private final String Coalesce(String... strings) {
    return Arrays.stream(strings)
        .filter(Objects::nonNull)
        .filter(s -> !s.isEmpty())
        .findFirst()
        .orElse(null);
  }

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    // If this is a "route" relation ...
    if (!relation.hasTag("type", "route")) {
      return null;
    }
    // where route=mtb/bicycle/hiking/foot ...
    if (!relation.hasTag("route", "mtb", "bicycle", "hiking", "foot")) {
      return null;
    }
    if (relation.hasTag("state", "proposed")) {
      return null;
    }
    // then store a RouteRelationInfo instance with tags we'll need later
    var members_ids = relation.members()
        .stream()
        .filter(member -> member.type() == WAY)
        .mapToLong(OsmElement.Relation.Member::ref)
        .boxed()
        .collect(Collectors.toList());
    if (members_ids.size() == 0) {
      return null;
    }
    // LOGGER.debug("The first member of route relation {}: way {}", relation.id(), members_ids.get(0));
    return List.of(new RouteRelationInfo(
      relation.id(),
      relation.getString("name"),
      relation.getString("name:he"),
      relation.getString("name:en"),
      relation.getString("route"),
      relation.getString("network"),
      relation.getString("ref"),
      relation.getString("osmc:symbol"),
      relation.getString("colour"),
      relation.getString("description"),
      relation.getString("description:he"),
      relation.getString("description:en"),
      relation.getString("wikidata"),
      relation.getString("image"),
      relation.getString("wikimedia_commons"),
      relation.hasTag("route", "mtb", "bicycle") ? "Bicycle" : "Hiking",
      members_ids.isEmpty() ? -1L : members_ids.get(0),
      members_ids
    ));
  }

  /*
   * Step 2)
   *
   * On the second pass through the input .osm.pbf file, for each way in a relation that we stored data about, emit a
   * point with attributes derived from the relation as well as for ways with mtb:name tag.
   */

  @Override
  public void preprocessOsmWay(OsmElement.Way way) {
    if (!way.hasTag("mtb:name")) {
      return;
    }
    String mtbName = way.getString("mtb:name");
    if (!Singles.containsKey(mtbName)) {
      var list = new ArrayList<Long>();
      list.add(way.id());
      Singles.put(mtbName, new SinglesIds(list, way.id()));
    } else {
      Singles.get(mtbName).ids.add(way.id());
      if (way.id() < Singles.get(mtbName).minId) {
        Singles.get(mtbName).minId = way.id();
      }
    }
  }

  private static final String trailPoisLayerName = "global_points";
  private static final String trailLayerName = "trail";

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // ignore nodes and ways that should only be treated as polygons
    if (sourceFeature.canBeLine()) {
      processOsmRelationFeature(sourceFeature, features);
      processMtbNameFeature(sourceFeature, features);
      processTrailFeature(sourceFeature, features);
    }
  }

  private void processOsmRelationFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // get all the RouteRelationInfo instances we returned from preprocessOsmRelation that
    // this way belongs to
    for (var routeInfo : sourceFeature.relationInfo(RouteRelationInfo.class)) {
      // (routeInfo.role() also has the "role" of this relation member if needed)
      RouteRelationInfo relation = routeInfo.relation();
      if (relation.name == null) {
        continue;
      }
      // Collect all relation way members
      if (!RelationLineMergers.containsKey(relation.id())) {
        RelationLineMergers.put(relation.id(), new MergedLinesHelper());
      }
      var mergedLines = RelationLineMergers.get(relation.id());
      synchronized(mergedLines) {
        try {
          mergedLines.lineMerger.add(sourceFeature.line());
          relation.member_ids.remove(sourceFeature.id());

          if (relation.first_member_id == sourceFeature.id()) {
            mergedLines.feature = sourceFeature;
          }

          if (!relation.member_ids.isEmpty()) {
            continue;
          }
          // All relation members were reached. Add a POI element for trail relation
          // The first segment of the members' linestrings merge
          var mergedLineStrings = mergedLines.lineMerger.getMergedLineStrings();
          // LOGGER.debug("Route relation {} was merged into {} LineStrings", relation.id(), mergedLineStrings.size());
          var firstLineString = (LineString) mergedLineStrings.iterator().next();
          // The first point in the first segment of the merge - the default location of the poi
          var point = firstLineString.getCoordinate();
          // The last point in the the first segment of the merge - the alternative location of the poi
          var lastMergedPoint = firstLineString.getCoordinateN(firstLineString.getNumPoints() - 1);
          // LOGGER.debug("The first merged segment of route relation {} starts at {} and ends at {}", relation.id(), point, lastMergedPoint);
          // The first node of the relation's first member
          var firstMemberGeometry = (LineString) mergedLines.feature.line();
          var firstMemberStart = firstMemberGeometry.getCoordinate();
          // The last node of the relation's first member
          var firstMemberEnd = firstMemberGeometry.getCoordinateN(firstMemberGeometry.getNumPoints() - 1);
          // LOGGER.debug("The first member of route relation {} starts at {} and end at {}", relation.id(), firstMemberStart, firstMemberEnd);
          if ( ! point.equals(firstMemberStart) &&
	      (lastMergedPoint.equals(firstMemberStart) || lastMergedPoint.equals(firstMemberEnd))) {
            // Place the poi at the last point in the the first segment of the merge
            point = lastMergedPoint;
            // LOGGER.debug("The poi for route relation {} was changed to {}", relation.id(), point);
          }
          features.geometry(trailPoisLayerName, GeoUtils.point(point))
            .setAttr("name", relation.name)
            .setAttr("name:he", Coalesce(relation.name_he, relation.name))
            .setAttr("name:en", Coalesce(relation.name_en, relation.name))
            .setAttr("description", relation.description)
            .setAttr("description:he", Coalesce(relation.description_he, relation.description))
            .setAttr("description:en", Coalesce(relation.description_en, relation.description))
            .setAttr("wikidata", relation.wikidata)
            .setAttr("image", relation.image)
            .setAttr("wikimedia_commons", relation.wikimedia_commons)
            .setAttr("route", relation.route)
            .setAttr("network", relation.network)
            .setAttr("poiCategory", relation.route_type)
            .setAttr("poiIcon", relation.route_type == "Bicycle" ? "icon-bike" : "icon-hike")
            .setAttr("poiIconColor", "black")
            .setZoomRange(10, 14)
            .setId(relation.vectorTileFeatureId(config.featureSourceIdMultiplier()));
        } catch (GeometryException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void processMtbNameFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (!sourceFeature.hasTag("mtb:name")) {
      return;
    }
    String mtbName = sourceFeature.getString("mtb:name");
    if (!Singles.containsKey(mtbName)) {
      return;
    }
    var minId = Singles.get(mtbName).minId;
    if (!SinglesLineMergers.containsKey(minId)) {
      SinglesLineMergers.put(minId, new MergedLinesHelper());
    }
    var mergedLines = SinglesLineMergers.get(minId);
    synchronized(mergedLines) {
      try {

        mergedLines.lineMerger.add(sourceFeature.worldGeometry());
        Singles.get(mtbName).ids.remove(sourceFeature.id());

        if (minId == sourceFeature.id()) {
          mergedLines.feature = sourceFeature;
        }
        if (!Singles.get(mtbName).ids.isEmpty()) {
          return;
        }
        var feature = mergedLines.feature;
        // This was the last way with the same mtb:name, so we can merge the lines and add the feature
        // Add a POI element for a SingleTrack
        var point = GeoUtils.point(((Geometry)mergedLines.lineMerger.getMergedLineStrings().iterator().next()).getCoordinate());
        features.geometry(trailPoisLayerName, point)
          .setAttr("mtb:name", mtbName)
          .setAttr("mtb:name:he", feature.getString("mtb:name:he"))
          .setAttr("mtb:name:en", feature.getString("mtb:name:en"))
          .setAttr("name", feature.getString("name"))
          .setAttr("name:he", Coalesce(feature.getString("name:he"), feature.getString("name"), feature.getString("mtb:name:he"), feature.getString("mtb:name")))
          .setAttr("name:en", Coalesce(feature.getString("name:en"), feature.getString("name"), feature.getString("mtb:name:en"), feature.getString("mtb:name")))
          .setAttr("description", feature.getString("description"))
          .setAttr("description:he", Coalesce(feature.getString("description:he"), feature.getString("description")))
          .setAttr("description:en", Coalesce(feature.getString("description:en"), feature.getString("description")))
          .setAttr("wikidata", feature.getString("wikidata"))
          .setAttr("wikimedia_commons", feature.getString("wikimedia_commons"))
          .setAttr("image", feature.getString("image"))
          .setAttr("poiIcon", "icon-bike")
          .setAttr("poiIconColor", "gray")
          .setAttr("poiCategory", "Bicycle")
          .setZoomRange(10, 14)
          // Override the feature id with the minimal id of the group
          .setId(feature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
      } catch (GeometryException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private String wayColour(String wayColour, String OsmcSymbol) {
    if (OsmcSymbol == null || wayColour == null) {
      return null;
    }
    if (OsmcSymbol.equals(wayColour + ":white:" + wayColour + "_stripe")) {
      return wayColour;
    }
    return null;
  }

  private void processTrailFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // get all the RouteRelationInfo instances we returned from preprocessOsmRelation that
    // this way belongs to
    for (var routeInfo : sourceFeature.relationInfo(RouteRelationInfo.class)) {
      // (routeInfo.role() also has the "role" of this relation member if needed)
      RouteRelationInfo relation = routeInfo.relation();
      // Add route segment element
      try {
        features.geometry(trailLayerName, sourceFeature.line())
          .setAttr("class", relation.route)
          .setAttr("network", relation.network)
          .setAttr("name", relation.name)
          .setAttr("name:he", Coalesce(relation.name_he, relation.name))
          .setAttr("name:en", Coalesce(relation.name_en, relation.name))
          .setAttr("ref", relation.ref)
          .setAttr("osmc_symbol", relation.osmc_symbol)
          .setAttr("colour", relation.colour)
          .setAttr("way_colour", wayColour(sourceFeature.getString("colour"), relation.osmc_symbol))
          .setAttr("highway", sourceFeature.getString("highway"))
          .setZoomRange(7, 14)
          .setId(relation.vectorTileFeatureId(config.featureSourceIdMultiplier()));
      } catch (GeometryException e) {
        throw new RuntimeException(e);
      }
    }
  }


  /*
   * Hooks to override metadata values in the output mbtiles file. Only name is required, the rest are optional. Bounds,
   * center, minzoom, maxzoom are set automatically based on input data and planetiler config.
   *
   * See: https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata)
   */

  @Override
  public String name() {
    return "Trails POIs overlay";
  }

  @Override
  public String description() {
    return "Overlay for walking and bicycle routes";
  }

  @Override
  public boolean isOverlay() {
    return true; // when true sets type=overlay, otherwise type=baselayer
  }

  /*
   * Any time you use OpenStreetMap data, you must ensure clients display the following copyright. Most clients will
   * display this automatically if you populate it in the attribution metadata in the mbtiles file:
   */
  @Override
  public String attribution() {
    return """
      <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
      """.trim();
  }
}
