package com.example;

import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.WAY;

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
import org.locationtech.jts.geom.Point;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalSearchProfile implements Profile {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalSearchProfile.class);
  private PlanetilerConfig config;
  private ElasticsearchClient esClient;

  public static final String POINTS_LAYER_NAME = "global_points";

  private static final Map<String, MinWayIdFinder> Singles = new HashMap<>();
  private static final Map<String, MinWayIdFinder> Waterways = new HashMap<>();
  private static final ConcurrentMap<Long, MergedLinesHelper> RelationLineMergers = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Long, MergedLinesHelper> WaysLineMergers = new ConcurrentHashMap<>();

  public GlobalSearchProfile(PlanetilerConfig config, ElasticsearchClient esClient) {
    this.config = config;
    this.esClient = esClient;
  }

  /*
   * The processing happens in 3 steps:
   * 1. On the first pass through the input file, store relevant information from applicable OSM route relations and ways with mtb:name tag.
   * 2. On the second pass, emit points for relation and mtb:name ways. Emit a point by merging all the ways and using the first point of the merged linestring.
   * 
   * Step 1)
   *
   * Planetiler processes the .osm.pbf input file in two passes. The first pass stores node locations, and invokes
   * preprocessOsmRelation for reach relation and stores information the profile needs during the second pass when we
   * emit map feature for ways contained in that relation.
   * 
   * Step 2)
   *
   * On the second pass through the input .osm.pbf file, for each way in a relation that we stored data about, emit a
   * point with attributes derived from the relation as well as for ways with mtb:name tag.
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

  @Override
  public void preprocessOsmWay(OsmElement.Way way) {
    if (way.hasTag("mtb:name")) {
      String mtbName = way.getString("mtb:name");
      if (!Singles.containsKey(mtbName)) {
        var finder = new MinWayIdFinder();
        finder.addWayId(way.id());
        Singles.put(mtbName, finder);
      } else {
        Singles.get(mtbName).addWayId(way.id());
      }
      return;
    }
    if (way.hasTag("waterway")) {
      String waterway = way.getString("waterway");
      if (!Waterways.containsKey(waterway)) {
        var finder = new MinWayIdFinder();
        finder.addWayId(way.id());
        Waterways.put(waterway, finder);
      } else {
        Waterways.get(waterway).addWayId(way.id());
      }
    }
    
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // ignore nodes and ways that should only be treated as polygons
    if (sourceFeature.canBeLine()) {
      processOsmRelationFeature(sourceFeature, features);
      processMtbNameFeature(sourceFeature, features);
      processWaterwayFeature(sourceFeature, features);
    } else {
      processOtherPoints(sourceFeature, features);
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
          var point = getFirstPointOfTrailRelation(mergedLines);

          var pointDocument = new PointDocument();
          pointDocument.name = relation.name;
          pointDocument.name_he = Coalesce(relation.name_he, relation.name);
          pointDocument.name_en = Coalesce(relation.name_en, relation.name);
          pointDocument.description = relation.description;
          pointDocument.description_he = Coalesce(relation.description_he, relation.description);
          pointDocument.description_en = Coalesce(relation.description_en, relation.description);
          pointDocument.wikidata = relation.wikidata;
          pointDocument.image = relation.image;
          pointDocument.wikimedia_commons = relation.wikimedia_commons;
          pointDocument.poiCategory = relation.route_type;
          pointDocument.poiIcon = relation.route_type == "Bicycle" ? "icon-bike" : "icon-hike";
          pointDocument.poiIconColor = "black";
          pointDocument.location = List.of(point.getX(), point.getY());

          insertToElasticsearch(pointDocument, "OSM_relation_" + relation.id);

          features.geometry(POINTS_LAYER_NAME, point)
            .setAttr("name", pointDocument.name)
            .setAttr("name:he", pointDocument.name_he)
            .setAttr("name:en", pointDocument.name_en)
            .setAttr("description", pointDocument.description)
            .setAttr("description:he", pointDocument.description_he)
            .setAttr("description:en", pointDocument.description_en)
            .setAttr("wikidata", pointDocument.wikidata)
            .setAttr("image", pointDocument.image)
            .setAttr("wikimedia_commons", relation.wikimedia_commons)
            .setAttr("route", relation.route)
            .setAttr("network", relation.network)
            .setAttr("poiCategory", pointDocument.poiCategory)
            .setAttr("poiIcon", pointDocument.poiIcon)
            .setAttr("poiIconColor", pointDocument.poiIconColor)
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
    if (!WaysLineMergers.containsKey(minId)) {
      WaysLineMergers.put(minId, new MergedLinesHelper());
    }
    var mergedLines = WaysLineMergers.get(minId);
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
        var point = GeoUtils.point(((Geometry)mergedLines.lineMerger.getMergedLineStrings().iterator().next()).getCoordinate());

        var pointDocument = new PointDocument();
        pointDocument.name = mtbName;
        pointDocument.name_he = Coalesce(feature.getString("name:he"), feature.getString("mtb:name:he"), feature.getString("name"), feature.getString("mtb:name"));
        pointDocument.name_en = Coalesce(feature.getString("name:en"), feature.getString("mtb:name:en"), feature.getString("name"), feature.getString("mtb:name"));
        pointDocument.description = feature.getString("description");
        pointDocument.description_he = Coalesce(feature.getString("description:he"), feature.getString("description"));
        pointDocument.description_en = Coalesce(feature.getString("description:en"), feature.getString("description"));
        pointDocument.wikidata = feature.getString("wikidata");
        pointDocument.image = feature.getString("image");
        pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
        pointDocument.poiCategory = "Bicycle";
        pointDocument.poiIcon = "icon-bike";
        pointDocument.poiIconColor = "gray";
        pointDocument.location = List.of(point.getX(), point.getY());

        insertToElasticsearch(pointDocument, "OSM_way_" + minId);
        // This was the last way with the same mtb:name, so we can merge the lines and add the feature
        // Add a POI element for a SingleTrack
        features.geometry(POINTS_LAYER_NAME, point)
          .setAttr("mtb:name", mtbName)
          .setAttr("mtb:name:he", feature.getString("mtb:name:he"))
          .setAttr("mtb:name:en", feature.getString("mtb:name:en"))
          .setAttr("name", pointDocument.name)
          .setAttr("name:he", pointDocument.name_he)
          .setAttr("name:en", pointDocument.name_en)
          .setAttr("description", pointDocument.description)
          .setAttr("description:he", pointDocument.description_he)
          .setAttr("description:en", pointDocument.description_en)
          .setAttr("wikidata", pointDocument.wikidata)
          .setAttr("wikimedia_commons", pointDocument.wikimedia_commons)
          .setAttr("image", pointDocument.image)
          .setAttr("poiIcon", pointDocument.poiIcon)
          .setAttr("poiIconColor", pointDocument.poiIconColor)
          .setAttr("poiCategory", pointDocument.poiCategory)
          .setZoomRange(10, 14)
          // Override the feature id with the minimal id of the group
          .setId(feature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
      } catch (GeometryException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void processWaterwayFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (!sourceFeature.hasTag("waterway")) {
      return;
    }
    String name = sourceFeature.getString("name");
    if (!Waterways.containsKey(name)) {
      return;
    }
    var minId = Waterways.get(name).minId;
    if (!WaysLineMergers.containsKey(minId)) {
      WaysLineMergers.put(minId, new MergedLinesHelper());
    }
    var mergedLines = WaysLineMergers.get(minId);
    synchronized(mergedLines) {
      try {

        mergedLines.lineMerger.add(sourceFeature.worldGeometry());
        Waterways.get(name).ids.remove(sourceFeature.id());

        if (minId == sourceFeature.id()) {
          mergedLines.feature = sourceFeature;
        }
        if (!Waterways.get(name).ids.isEmpty()) {
          return;
        }
        var feature = mergedLines.feature;
        var point = GeoUtils.point(((Geometry)mergedLines.lineMerger.getMergedLineStrings().iterator().next()).getCoordinate());

        var pointDocument = new PointDocument();
        pointDocument.name = name;
        pointDocument.name_he = Coalesce(feature.getString("name:he"), feature.getString("name"));
        pointDocument.name_en = Coalesce(feature.getString("name:en"), feature.getString("name"));
        pointDocument.description = feature.getString("description");
        pointDocument.description_he = Coalesce(feature.getString("description:he"), feature.getString("description"));
        pointDocument.description_en = Coalesce(feature.getString("description:en"), feature.getString("description"));
        pointDocument.wikidata = feature.getString("wikidata");
        pointDocument.image = feature.getString("image");
        pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
        pointDocument.poiCategory = "Water";
        pointDocument.poiIcon = "icon-waterfall";
        pointDocument.poiIconColor = "blue";
        pointDocument.location = List.of(point.getX(), point.getY());

        insertToElasticsearch(pointDocument, "OSM_way_" + minId);
        if (!isInterestingPoint(pointDocument)) {
          // Skip adding features without any description or image to tiles
          return;
        }
        
        features.geometry(POINTS_LAYER_NAME, point)
          .setAttr("name", pointDocument.name)
          .setAttr("name:he", pointDocument.name_he)
          .setAttr("name:en", pointDocument.name_en)
          .setAttr("description", pointDocument.description)
          .setAttr("description:he", pointDocument.description_he)
          .setAttr("description:en", pointDocument.description_en)
          .setAttr("wikidata", pointDocument.wikidata)
          .setAttr("wikimedia_commons", pointDocument.wikimedia_commons)
          .setAttr("image", pointDocument.image)
          .setAttr("poiIcon", pointDocument.poiIcon)
          .setAttr("poiIconColor", pointDocument.poiIconColor)
          .setAttr("poiCategory", pointDocument.poiCategory)
          .setZoomRange(10, 14)
          // Override the feature id with the minimal id of the group
          .setId(feature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
      } catch (GeometryException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void processOtherPoints(SourceFeature feature, FeatureCollector features) {
    if (!feature.hasTag("name") && 
        !feature.hasTag("wikidata") && 
        !feature.hasTag("image") && 
        !feature.hasTag("description") &&
        !feature.hasTag("ref:IL:inature")) {
      return;
    }

    var tileId = feature.vectorTileFeatureId(config.featureSourceIdMultiplier());
    var docId = "OSM_" + (String.valueOf(tileId).endsWith("1") ? "node_" : String.valueOf(tileId).endsWith("2") ? "way_" : "relation_") + feature.id();
    Point point;
    
    try {
        point = (Point)feature.centroidIfConvex();
    } catch (GeometryException e) {
      try {
        point = GeoUtils.point(feature.worldGeometry().getCoordinate());
      } catch (GeometryException e2) {
        LOGGER.warn("Failed to process feature: {} (this is usually a relations outside the area)", docId);
        return;
      }
    }

    var pointDocument = new PointDocument();
    pointDocument.name = feature.getString("name");
    pointDocument.name_he = Coalesce(feature.getString("name:he"), feature.getString("name"));
    pointDocument.name_en = Coalesce(feature.getString("name:en"), feature.getString("name"));
    pointDocument.description = feature.getString("description");
    pointDocument.description_he = Coalesce(feature.getString("description:he"), feature.getString("description"));
    pointDocument.description_en = Coalesce(feature.getString("description:en"), feature.getString("description"));
    pointDocument.wikidata = feature.getString("wikidata");
    pointDocument.image = feature.getString("image");
    pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
    pointDocument.location = List.of(point.getX(), point.getY());

    setIconColorCategory(pointDocument, feature);

    if (pointDocument.poiIcon == "icon-search") {
        return;
    }

    insertToElasticsearch(pointDocument, docId);

    if (pointDocument.poiIcon == "icon-peak" && !isInterestingPoint(pointDocument)) {
        return;
    }

    features.geometry(POINTS_LAYER_NAME, point)
        .setAttr("name", pointDocument.name)
        .setAttr("name:he", pointDocument.name_he)
        .setAttr("name:en", pointDocument.name_en)
        .setAttr("description", pointDocument.description)
        .setAttr("description:he", pointDocument.description_he)
        .setAttr("description:en", pointDocument.description_en)
        .setAttr("wikidata", pointDocument.wikidata)
        .setAttr("wikimedia_commons", pointDocument.wikimedia_commons)
        .setAttr("image", pointDocument.image)
        .setAttr("poiIcon", pointDocument.poiIcon)
        .setAttr("poiIconColor", pointDocument.poiIconColor)
        .setAttr("poiCategory", pointDocument.poiCategory)
        .setZoomRange(10, 14)
        .setId(tileId);
    
  }

  private void insertToElasticsearch(PointDocument pointDocument, String docId) {
    try {
      esClient.index(i -> i
          .index("points")
          .id(docId)
          .document(pointDocument)
      );
    } catch (Exception e) {
      // swallow
    }
  }

  /**
   * Get the first point of the trail relation by checking some heuristics related to the relation's first member
   * @param mergedLines - the merged lines helper
   * @return the first point of the trail relation
   * @throws GeometryException
   */
  private Point getFirstPointOfTrailRelation(MergedLinesHelper mergedLines) throws GeometryException {
    var firstMergedLineString = (LineString) mergedLines.lineMerger.getMergedLineStrings().iterator().next();
    var firstMergedLineCoordinate = firstMergedLineString.getCoordinate();
    var lastMergedLineCoordinate = firstMergedLineString.getCoordinateN(firstMergedLineString.getNumPoints() - 1);
    
    var firstMemberGeometry = (LineString) mergedLines.feature.line();
    var firstMemberStartCoordinate = firstMemberGeometry.getCoordinate();
    var firstMemberEndCoordinate = firstMemberGeometry.getCoordinateN(firstMemberGeometry.getNumPoints() - 1);

    if (firstMergedLineCoordinate.equals(firstMemberStartCoordinate)) {
      // The direction of the related's first memeber and the merged lines is the same
      return GeoUtils.point(firstMergedLineCoordinate);  
    }

    if (lastMergedLineCoordinate.equals(firstMemberStartCoordinate) || lastMergedLineCoordinate.equals(firstMemberEndCoordinate)) {
      // The direction of the related's first memeber and the merged lines is the opposite
      return GeoUtils.point(lastMergedLineCoordinate);
    }
    // Otherwise, return the first point of the merged line
    return GeoUtils.point(firstMergedLineCoordinate);
  }

  private boolean isInterestingPoint(PointDocument pointDocument) {
    return pointDocument.description != null || 
      pointDocument.description_en != null || 
      pointDocument.description_he != null ||
      pointDocument.wikidata != null ||
      pointDocument.image != null;
  }


  private void setIconColorCategory(PointDocument pointDocument, SourceFeature feature) {
    if ("protected_area".equals(feature.getString("boundary")) || 
        "national_park".equals(feature.getString("boundary")) ||
        "nature_reserve".equals(feature.getString("leisure"))) {
            pointDocument.poiIconColor = "#008000";
            pointDocument.poiIcon = "icon-nature-reserve";
            pointDocument.poiCategory = "Other";
        return;
    }
    if (feature.getString("network") != null) {
        switch (feature.getString("network")) {
            case "lcn":
            case "rcn":
                pointDocument.poiIconColor = "black";
                pointDocument.poiIcon = "icon-bike";
                pointDocument.poiCategory = "Bicycle";
                return;
            case "lwn":
            case "rwn":
                pointDocument.poiIconColor = "black";
                pointDocument.poiIcon = "icon-hike";
                pointDocument.poiCategory = "Hiking";
                return;
        }
    }
    if (feature.getString("route") != null) {
        switch (feature.getString("route")) {
            case "hiking":
                pointDocument.poiIconColor = "black";
                pointDocument.poiIcon = "icon-hike";
                pointDocument.poiCategory = "Hiking";
                return;
            case "bicycle":
                pointDocument.poiIconColor = "black";
                pointDocument.poiIcon = "icon-bike";
                pointDocument.poiCategory = "Bicycle";
                return;
        }
    }
    if (feature.getString("historic") != null) {
        pointDocument.poiIconColor = "#666666";
        pointDocument.poiCategory = "Historic";
        switch (feature.getString("historic")) {
            case "ruins":
                pointDocument.poiIcon = "icon-ruins";
                return;
            case "archaeological_site":
                pointDocument.poiIcon = "icon-archaeological";
                return;
            case "memorial":
            case "monument":
                pointDocument.poiIcon = "icon-memorial";
                return;
            case "tomb":
                pointDocument.poiIconColor = "black";
                pointDocument.poiIcon = "icon-cave";
                pointDocument.poiCategory = "Natural";
                return;
        }
    }
    if ("picnic_table".equals(feature.getString("leisure")) || 
        "picnic_site".equals(feature.getString("tourism")) || 
        "picnic".equals(feature.getString("amenity"))) {
        pointDocument.poiIconColor = "#734a08";
        pointDocument.poiIcon = "icon-picnic";
        pointDocument.poiCategory = "Camping";
        return;
    }

    if (feature.getString("natural") != null) {
        switch (feature.getString("natural")) {
            case "cave_entrance":
                pointDocument.poiIconColor = "black";
                pointDocument.poiIcon = "icon-cave";
                pointDocument.poiCategory = "Natural";
                return;
            case "spring":
                pointDocument.poiIconColor = "blue";
                pointDocument.poiIcon = "icon-tint";
                pointDocument.poiCategory = "Water";
                return;
            case "tree":
                pointDocument.poiIconColor = "#008000";
                pointDocument.poiIcon = "icon-tree";
                pointDocument.poiCategory = "Natural";
                return;
            case "flowers":
                pointDocument.poiIconColor = "#008000";
                pointDocument.poiIcon = "icon-flowers";
                pointDocument.poiCategory = "Natural";
                return;
            case "waterhole":
                pointDocument.poiIconColor = "blue";
                pointDocument.poiIcon = "icon-waterhole";
                pointDocument.poiCategory = "Water";
                return;
        }
    }

    if ("reservoir".equals(feature.getString("water")) || 
        "pond".equals(feature.getString("water"))) {
        pointDocument.poiIconColor = "blue";
        pointDocument.poiIcon = "icon-tint";
        pointDocument.poiCategory = "Water";
        return;
    }

    if (feature.getString("man_made") != null) {
        pointDocument.poiIconColor = "blue";
        pointDocument.poiCategory = "Water";
        switch (feature.getString("man_made")) {
            case "water_well":
                pointDocument.poiIcon = "icon-water-well";
                return;
            case "cistern":
                pointDocument.poiIcon = "icon-cistern";
                return;
        }
    }

    if ("waterfall".equals(feature.getString("waterway"))) {
        pointDocument.poiIconColor = "blue";
        pointDocument.poiIcon = "icon-waterfall";
        pointDocument.poiCategory = "Water";
        return;
    }

    if (feature.getString("place") != null) {
        pointDocument.poiIconColor = "black";
        pointDocument.poiIcon = "icon-home";
        pointDocument.poiCategory = "Wikipedia";
        return;
    }

    if (feature.getString("tourism") != null) {
        switch (feature.getString("tourism")) {
            case "viewpoint":
                pointDocument.poiIconColor = "#008000";
                pointDocument.poiIcon = "icon-viewpoint";
                pointDocument.poiCategory = "Viewpoint";
                return;
            case "camp_site":
                pointDocument.poiIconColor = "#734a08";
                pointDocument.poiIcon = "icon-campsite";
                pointDocument.poiCategory = "Camping";
                return;
            case "attraction":
                pointDocument.poiIconColor = "#ffb800";
                pointDocument.poiIcon = "icon-star";
                pointDocument.poiCategory = "Other";
                return;
        }
    }

    if ("peak".equals(feature.getString("natural"))) {
        pointDocument.poiIconColor = "black";
        pointDocument.poiIcon = "icon-peak";
        pointDocument.poiCategory = "Natural";
        return;
    }

    if (feature.getString("ref:IL:inature") != null) {
        pointDocument.poiIconColor = "#116C00";
        pointDocument.poiIcon = "icon-inature";
        pointDocument.poiCategory = "iNature";
        return;
    }

    if (feature.getString("wikidata") != null || feature.getString("wikipedia") != null) {
        pointDocument.poiIconColor = "black";
        pointDocument.poiIcon = "icon-wikipedia-w";
        pointDocument.poiCategory = "Wikipedia";
        return;
    }

    pointDocument.poiIconColor = "black";
    pointDocument.poiIcon = "icon-search";
    pointDocument.poiCategory = "Other";
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
