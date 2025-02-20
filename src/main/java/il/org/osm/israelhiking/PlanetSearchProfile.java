package il.org.osm.israelhiking;

import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.WAY;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlanetSearchProfile implements Profile {
  private static final Logger LOGGER = LoggerFactory.getLogger(PlanetSearchProfile.class);
  private PlanetilerConfig config;
  private ElasticsearchClient esClient;
  private final String pointsIndexName;
  private final String bboxIndexName;
  private final String[] supportedLanguages;

  public static final String POINTS_LAYER_NAME = "global_points";

  private static final Map<String, MinWayIdFinder> Singles = new HashMap<>();
  private static final Map<String, MinWayIdFinder> Waterways = new HashMap<>();
  private static final ConcurrentMap<Long, MergedLinesHelper> RelationLineMergers = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Long, MergedLinesHelper> WaysLineMergers = new ConcurrentHashMap<>();

  public PlanetSearchProfile(PlanetilerConfig config, ElasticsearchClient esClient, String pointsIndexName, String bboxIndexName, String[] supportedLnaguages) {
    this.config = config;
    this.esClient = esClient;
    this.pointsIndexName = pointsIndexName;
    this.supportedLanguages = supportedLnaguages;
    this.bboxIndexName = bboxIndexName;
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

  static private final void CoalesceIntoMap(Map<String, String> map, String language, String... strings) {
    var value = Arrays.stream(strings)
      .filter(Objects::nonNull)
      .filter(s -> !s.isEmpty())
      .findFirst()
      .orElse(null);
    if (value != null) {
      map.put(language, value);
    }
  }

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    // If this is a "route" relation ...
    if (relation.hasTag("state", "proposed")) {
      return null;
    }
    var pointDocument = new PointDocument();
    setIconColorCategory(pointDocument, relation);

    if (!"icon-waterfall".equals(pointDocument.poiIcon) && 
        !"Bicycle".equals(pointDocument.poiCategory) && 
        !"Hiking".equals(pointDocument.poiCategory)) {
      return null;
    }
    // then store a RouteRelationInfo instance with tags we'll need later
    var members_ids = relation.members()
        .stream()
        .filter(member -> member.type() == WAY)
        .mapToLong(OsmElement.Relation.Member::ref)
        .boxed()
        .collect(Collectors.toList());
    if (members_ids.isEmpty()) {
      return null;
    }
    var info = new RelationInfo(relation.id());
    
    for (String language : supportedLanguages) {
      CoalesceIntoMap(pointDocument.name, language, relation.getString("name:" + language), relation.getString("name"));
      CoalesceIntoMap(pointDocument.description, language, relation.getString("description:" + language), relation.getString("description"));
    }
    pointDocument.poiSource = "OSM";
    pointDocument.wikidata = relation.getString("wikidata");
    pointDocument.image = relation.getString("image");
    pointDocument.wikimedia_commons = relation.getString("wikimedia_commons");
    info.pointDocument = pointDocument;
    info.firstMemberId = members_ids.getFirst();
    info.memberIds = members_ids;

    return List.of(info);
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
      String waterwayName = way.getString("name");
      if (!Waterways.containsKey(waterwayName)) {
        var finder = new MinWayIdFinder();
        finder.addWayId(way.id());
        Waterways.put(waterwayName, finder);
      } else {
        Waterways.get(waterwayName).addWayId(way.id());
      }
    }
    
  }

  @Override
  public void processFeature(SourceFeature feature, FeatureCollector features) {
    try {
      if (feature.getSource() == "external") {
        processExternalFeautre(feature, features);
        return;
      }
      if (isBBoxFeature(feature, supportedLanguages)) {
        insertBboxToElasticsearch(feature, supportedLanguages);
      }
      processOsmRelationFeature(feature, features);
      if (processMtbNameFeature(feature, features)) return;
      if (processWaterwayFeature(feature, features)) return;
      if (processHighwayFeautre(feature, features)) return;
      if (processOtherSourceFeature(feature, features)) return;
      addNonIconFeaturesToElasricseach(feature);
    } catch (GeometryException e) {
      // ignore bad geometries
    }
  }

  private void processExternalFeautre(SourceFeature feature, FeatureCollector features) throws GeometryException {
    var pointDocument = new PointDocument();
    pointDocument.poiIcon = feature.getString("poiIcon");
    pointDocument.poiIconColor = feature.getString("poiIconColor");
    pointDocument.poiCategory = feature.getString("poiCategory");
    for (String language : supportedLanguages) {
      CoalesceIntoMap(pointDocument.name, language, feature.getString("name:" + language), feature.getString("name"));
      CoalesceIntoMap(pointDocument.description, language, feature.getString("description:" + language), feature.getString("description"));
    }
    pointDocument.poiSource = feature.getString("poiSource");
    pointDocument.wikidata = feature.getString("wikidata");
    pointDocument.image = feature.getString("image");
    pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
    var point = feature.canBePolygon() ? (Point)feature.centroidIfConvex() : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var docId = pointDocument.poiSource + "_" + feature.getString("identifier");
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};
    
    insertPointToElasticsearch(pointDocument, docId);
    
    var tileFeature = features.geometry("external", point)
        .setAttr("poiId", docId)
        .setAttr("identifier", feature.getString("identifier"))
        .setZoomRange(10, 14)
        .setId(feature.id());
    setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
  }

  private void processOsmRelationFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    // get all the RouteRelationInfo instances we returned from preprocessOsmRelation that
    // this way belongs to
    for (var routeInfo : feature.relationInfo(RelationInfo.class)) {
      // (routeInfo.role() also has the "role" of this relation member if needed)
      RelationInfo relation = routeInfo.relation();
      if (relation.pointDocument.name.isEmpty()) {
        continue;
      }
      // Collect all relation way members
      if (!RelationLineMergers.containsKey(relation.id())) {
        RelationLineMergers.put(relation.id(), new MergedLinesHelper());
      }
      var mergedLines = RelationLineMergers.get(relation.id());
      synchronized(mergedLines) {
        mergedLines.lineMerger.add(feature.line());
        relation.memberIds.remove(feature.id());

        if (relation.firstMemberId == feature.id()) {
          mergedLines.feature = feature;
        }

        if (!relation.memberIds.isEmpty()) {
          continue;
        }
        // All relation members were reached. Add a POI element for line relation
        var point = GeoUtils.point(mergedLines.lineMerger.getFirstPoint());
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        relation.pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};

        insertPointToElasticsearch(relation.pointDocument, "OSM_relation_" + relation.id());

        var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
          .setZoomRange(10, 14)
          .setId(relation.vectorTileFeatureId(config.featureSourceIdMultiplier()));
        setFeaturePropertiesFromPointDocument(tileFeature, relation.pointDocument);
      }
    }
  }

  private boolean processMtbNameFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!feature.hasTag("mtb:name")) {
      return false;
    }
    String mtbName = feature.getString("mtb:name");
    if (!Singles.containsKey(mtbName)) {
      return false;
    }
    var minId = Singles.get(mtbName).minId;
    if (!WaysLineMergers.containsKey(minId)) {
      WaysLineMergers.put(minId, new MergedLinesHelper());
    }
    var mergedLines = WaysLineMergers.get(minId);
    synchronized(mergedLines) {
      mergedLines.lineMerger.add(feature.worldGeometry());
      Singles.get(mtbName).ids.remove(feature.id());

      if (minId == feature.id()) {
        mergedLines.feature = feature;
      }
      if (!Singles.get(mtbName).ids.isEmpty()) {
        return true;
      }
      var minIdFeature = mergedLines.feature;
      var point = GeoUtils.point(mergedLines.lineMerger.getFirstPoint());

      var pointDocument = new PointDocument();
      for (String language : supportedLanguages) {
        CoalesceIntoMap(pointDocument.name, language, minIdFeature.getString("mtb:name:" + language), minIdFeature.getString("name:" + language), minIdFeature.getString("name"), minIdFeature.getString("mtb:name"));
        CoalesceIntoMap(pointDocument.description, language, minIdFeature.getString("description:" + language), minIdFeature.getString("description"));
      }
      pointDocument.wikidata = minIdFeature.getString("wikidata");
      pointDocument.image = minIdFeature.getString("image");
      pointDocument.wikimedia_commons = minIdFeature.getString("wikimedia_commons");
      pointDocument.poiCategory = "Bicycle";
      pointDocument.poiIcon = "icon-bike";
      pointDocument.poiIconColor = "gray";
      pointDocument.poiSource = "OSM";
      var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
      pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};

      insertPointToElasticsearch(pointDocument, "OSM_way_" + minId);
      // This was the last way with the same mtb:name, so we can merge the lines and add the feature
      // Add a POI element for a SingleTrack
      var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
        .setZoomRange(10, 14)
        // Override the feature id with the minimal id of the group
        .setId(minIdFeature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
        setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
    }
    return true;
  }

  private boolean processWaterwayFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!feature.hasTag("waterway")) {
      return false;
    }
    String name = feature.getString("name");
    if (!Waterways.containsKey(name)) {
      return false;
    }
    for (var routeInfo : feature.relationInfo(RelationInfo.class)) {
      RelationInfo relation = routeInfo.relation();
      if (relation.pointDocument.poiIcon == "icon-waterfall") {
        // In case this waterway is part of a relation, we already processed it
        return true;
      }
    }

    var minId = Waterways.get(name).minId;
    if (!WaysLineMergers.containsKey(minId)) {
      WaysLineMergers.put(minId, new MergedLinesHelper());
    }
    var mergedLines = WaysLineMergers.get(minId);
    synchronized(mergedLines) {

      mergedLines.lineMerger.add(feature.worldGeometry());
      Waterways.get(name).ids.remove(feature.id());

      if (minId == feature.id()) {
        mergedLines.feature = feature;
      }
      if (!Waterways.get(name).ids.isEmpty()) {
        return true;
      }
      var minIdFeature = mergedLines.feature;
      var point = GeoUtils.point(mergedLines.lineMerger.getFirstPoint());

      var pointDocument = new PointDocument();
      for (String language : supportedLanguages) {
        CoalesceIntoMap(pointDocument.name, language, minIdFeature.getString("name:" + language), minIdFeature.getString("name"));
        CoalesceIntoMap(pointDocument.description, language, minIdFeature.getString("description:" + language), minIdFeature.getString("description"));
      }
      pointDocument.wikidata = minIdFeature.getString("wikidata");
      pointDocument.image = minIdFeature.getString("image");
      pointDocument.wikimedia_commons = minIdFeature.getString("wikimedia_commons");
      pointDocument.poiCategory = "Water";
      pointDocument.poiIcon = "icon-waterfall";
      pointDocument.poiIconColor = "blue";
      pointDocument.poiSource = "OSM";
      var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
      pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};

      insertPointToElasticsearch(pointDocument, "OSM_way_" + minId);
      if (!isInterestingPoint(pointDocument)) {
        // Skip adding features without any description or image to tiles
        return true;
      }
      
      var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
        .setZoomRange(10, 14)
        // Override the feature id with the minimal id of the group
        .setId(minIdFeature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
      setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);

      return true;
    }
  }

  private boolean processHighwayFeautre(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!feature.hasTag("highway") || !feature.hasTag("name")) {
      return false;
    }
    var point = GeoUtils.point(feature.worldGeometry().getCoordinate());
    var pointDocument = new PointDocument();
    for (String language : supportedLanguages) {
      CoalesceIntoMap(pointDocument.name, language, feature.getString("name:" + language), feature.getString("name"));
      CoalesceIntoMap(pointDocument.description, language, feature.getString("description:" + language), feature.getString("description"));
    }
    
    pointDocument.wikidata = feature.getString("wikidata");
    pointDocument.image = feature.getString("image");
    pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
    pointDocument.poiSource = "OSM";
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};
    setIconColorCategory(pointDocument, feature);

    if (pointDocument.poiIcon == "icon-search") {
      return true;
    }

    insertPointToElasticsearch(pointDocument, sourceFeatureToDocumentId(feature));
    return true;
  }

  private boolean processOtherSourceFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!feature.hasTag("name") && 
        !feature.hasTag("wikidata") && 
        !feature.hasTag("image") && 
        !feature.hasTag("description") &&
        !feature.hasTag("ref:IL:inature")) {
      return false;
    }

    var tileId = feature.vectorTileFeatureId(config.featureSourceIdMultiplier());
    var docId = sourceFeatureToDocumentId(feature);
    var point = feature.canBePolygon() ? (Point)feature.centroidIfConvex() : GeoUtils.point(feature.worldGeometry().getCoordinate());

    var pointDocument = new PointDocument();
    for (String language : supportedLanguages) {
      CoalesceIntoMap(pointDocument.name, language, feature.getString("name:" + language), feature.getString("name"));
      CoalesceIntoMap(pointDocument.description, language, feature.getString("description:" + language), feature.getString("description"));
    }
    pointDocument.wikidata = feature.getString("wikidata");
    pointDocument.image = feature.getString("image");
    pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
    pointDocument.poiSource = "OSM";
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};

    setIconColorCategory(pointDocument, feature);

    if (pointDocument.poiIcon == "icon-search" || 
      (pointDocument.poiIcon == "icon-home" && (!isInterestingPoint(pointDocument) || !feature.isPoint()))) {
        return false;
    }

    insertPointToElasticsearch(pointDocument, docId);

    if ((pointDocument.poiIcon == "icon-peak" || pointDocument.poiIcon == "icon-waterfall") && !isInterestingPoint(pointDocument)) {
        return true;
    }

    var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
        .setZoomRange(10, 14)
        .setId(tileId);

    setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
    return true;
  }

  private void addNonIconFeaturesToElasricseach(SourceFeature feature) throws GeometryException {
    if (!feature.hasTag("name")) {
      return;
    }
    var pointDocument = new PointDocument();
    if (feature.hasTag("amenity", "place_of_worship") ||
      feature.hasTag("natural", "valley")) {
      pointDocument.poiIcon = "icon-search";
      pointDocument.poiIconColor = "black";
      pointDocument.poiCategory = "Other";
    }
    if (feature.hasTag("railway", "station") ||
      feature.hasTag("aerialway", "station")) {
      pointDocument.poiIcon = "icon-bus-stop";
      pointDocument.poiIconColor = "black";
      pointDocument.poiCategory = "Other";
    }
    if (feature.hasTag("natural", "ridge")) {
      pointDocument.poiIcon = "icon-peak";
      pointDocument.poiIconColor = "black";
      pointDocument.poiCategory = "Other";
    }
    if ((feature.hasTag("landuse", "recreation_ground") && feature.hasTag("sport", "mtb"))) {
      pointDocument.poiIcon = "icon-bike";
      pointDocument.poiIconColor = "green";
      pointDocument.poiCategory = "Bicycle";
    }
    
    if (pointDocument.poiIcon == null) {
      return;
    }

    
    for (String language : supportedLanguages) {
      CoalesceIntoMap(pointDocument.name, language, feature.getString("name:" + language), feature.getString("name"));
      CoalesceIntoMap(pointDocument.description, language, feature.getString("description:" + language), feature.getString("description"));
    }
    pointDocument.wikidata = feature.getString("wikidata");
    pointDocument.image = feature.getString("image");
    pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
    pointDocument.poiSource = "OSM";
    var docId = sourceFeatureToDocumentId(feature);
    var point = feature.canBePolygon() ? (Point)feature.centroidIfConvex() : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};
    insertPointToElasticsearch(pointDocument, docId);
  }

  private void insertPointToElasticsearch(PointDocument pointDocument, String docId) {
    try {
      esClient.index(i -> i
          .index(this.pointsIndexName)
          .id(docId)
          .document(pointDocument)
      );
    } catch (Exception e) {
      // swallow
    }
  }

  private void insertBboxToElasticsearch(SourceFeature feature, String[] supportedLanguages) {
    Envelope envelope;
    try {
      envelope = feature.polygon().getEnvelopeInternal();
    } catch (GeometryException e) {
      return;
    }
    try {
      var bbox = new BBoxDocument();
      bbox.area = feature.areaMeters();

      bbox.setBBox(GeoUtils.toLatLonBoundsBounds(envelope));
      for (String lang : supportedLanguages) {
        CoalesceIntoMap(bbox.name, lang, feature.getString("name:" + lang), feature.getString("name"));
      }
      esClient.index(i -> i
          .index(this.bboxIndexName)
          .id(sourceFeatureToDocumentId(feature))
          .document(bbox)
      );
    } catch (Exception e) {
      // swallow
      LOGGER.error("Unable to insert: " + e.getMessage());
    }
  }


  private boolean isInterestingPoint(PointDocument pointDocument) {
    return !pointDocument.description.isEmpty() || 
      pointDocument.wikidata != null ||
      pointDocument.image != null;
  }

  private void setFeaturePropertiesFromPointDocument(Feature tileFeature, PointDocument pointDocument) {
    tileFeature.setAttr("wikidata", pointDocument.wikidata)
        .setAttr("wikimedia_commons", pointDocument.wikimedia_commons)
        .setAttr("image", pointDocument.image)
        .setAttr("poiIcon", pointDocument.poiIcon)
        .setAttr("poiIconColor", pointDocument.poiIconColor)
        .setAttr("poiCategory", pointDocument.poiCategory)
        .setAttr("poiSource", pointDocument.poiSource);
    for (String lang : supportedLanguages) {
      tileFeature.setAttr("name:" + lang, pointDocument.name.get(lang));
      tileFeature.setAttr("description:" + lang, pointDocument.description.get(lang));
    }
  }

  private boolean isBBoxFeature(SourceFeature feature, String[] supportedLanguages) {
      if (!feature.canBePolygon()) {
          return false;
      }
      var hasName = false;
      for (String language : supportedLanguages) {
          if (feature.hasTag("name:" + language)) {
              hasName = true;
              break;
          }
      }
      if (!feature.hasTag("name") && !hasName) {
          return false;
      }
      var isFeatureADecentCity = feature.hasTag("boundary", "administrative") &&
                                  feature.hasTag("admin_level") &&
                                  feature.getLong("admin_level") > 0 &&
                                  feature.getLong("admin_level") <= 8;
      if (isFeatureADecentCity) {
          return true;
      }
      if (feature.hasTag("place") && 
          !feature.hasTag("place", "suburb") &&
          !feature.hasTag("place", "neighbourhood") &&
          !feature.hasTag("place", "quarter") &&
          !feature.hasTag("place", "city_block") &&
          !feature.hasTag("place", "borough")
          ) {
          return true;
      }
      if (feature.hasTag("landuse", "forest")) {
          return true;
      }
      return feature.hasTag("leisure", "nature_reserve") ||
            feature.hasTag("boundary", "national_park") ||
            feature.hasTag("boundary", "protected_area");
  }

  private String sourceFeatureToDocumentId(SourceFeature feature) {
    var tileId = feature.vectorTileFeatureId(config.featureSourceIdMultiplier());
    return "OSM_" + (String.valueOf(tileId).endsWith("1") 
      ? "node_" 
      : String.valueOf(tileId).endsWith("2") 
        ? "way_" 
        : "relation_") + feature.id();
  }

  private void setIconColorCategory(PointDocument pointDocument, WithTags feature) {
    if ("protected_area".equals(feature.getString("boundary")) || 
        "national_park".equals(feature.getString("boundary")) ||
        "nature_reserve".equals(feature.getString("leisure"))) {
            pointDocument.poiIconColor = "#008000";
            pointDocument.poiIcon = "icon-nature-reserve";
            pointDocument.poiCategory = "Other";
        return;
    }
    if (feature.getString("route") != null) {
        switch (feature.getString("route")) {
            case "hiking":
            case "foot":
                pointDocument.poiIconColor = "black";
                pointDocument.poiIcon = "icon-hike";
                pointDocument.poiCategory = "Hiking";
                return;
            case "bicycle":
            case "mtb":
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
        "pond".equals(feature.getString("water")) ||
        "lake".equals(feature.getString("water")) || 
        "stream_pool".equals(feature.getString("water"))) {
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

    if ("waterfall".equals(feature.getString("waterway")) || "waterway".equals(feature.getString("type"))) {
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

    if (feature.getString("highway") != null) {
      switch (feature.getString("highway")) {
        case "cycleway":
          pointDocument.poiIconColor = "black";
          pointDocument.poiCategory = "Bicycle";
          pointDocument.poiIcon = "icon-bike";
          return;
        case "footway":
          pointDocument.poiIconColor = "black";
          pointDocument.poiCategory = "Hiking";
          pointDocument.poiIcon = "icon-hike";
          return;
        case "path":
          pointDocument.poiIconColor = "black";
          pointDocument.poiCategory = "Hiking";
          pointDocument.poiIcon = "icon-hike";
          return;
        case "track":
          pointDocument.poiIconColor = "black";
          pointDocument.poiCategory = "4x4";
          pointDocument.poiIcon = "icon-four-by-four";
          return;
      }
    }

    if (feature.getString("ref:IL:inature") != null) {
        pointDocument.poiIconColor = "#116C00";
        pointDocument.poiIcon = "icon-inature";
        pointDocument.poiCategory = "iNature";
        return;
    }

    if ((feature.getString("wikidata") != null || feature.getString("wikipedia") != null) &&
      feature.getString("highway") == null && feature.getString("boundary") == null) {
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
