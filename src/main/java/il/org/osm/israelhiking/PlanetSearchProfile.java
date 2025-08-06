package il.org.osm.israelhiking;

import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.WAY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
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

public class PlanetSearchProfile implements Profile {
  private PlanetilerConfig config;
  private ElasticsearchClient esClient;
  private final String pointsIndexName;
  private final String bboxIndexName;
  private final String[] supportedLanguages;

  public static final String POINTS_LAYER_NAME = "global_points";

  private static final Map<String, MinWayIdFinder> Singles = new ConcurrentHashMap<>();
  private static final Map<String, MinWayIdFinder> Waterways = new ConcurrentHashMap<>();

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

  private void convertTagsToDocument(PointDocument pointDocument, WithTags feature) {
    for (String language : supportedLanguages) {
      CoalesceIntoMap(pointDocument.name, language, feature.getString("name:" + language));
      CoalesceIntoMap(pointDocument.description, language, feature.getString("description:" + language));
    }
    if (feature.hasTag("name")) {
      CoalesceIntoMap(pointDocument.name, "default", feature.getString("name"));
    }
    pointDocument.wikidata = feature.getString("wikidata");
    pointDocument.image = feature.getString("image");
    pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
  }

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    // If this is a "route" relation ...
    if (relation.hasTag("state", "proposed")) {
      return null;
    }
    var pointDocument = new PointDocument();
    setIconColorCategory(pointDocument, relation);

    if (!"icon-river".equals(pointDocument.poiIcon) && 
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
    
    convertTagsToDocument(pointDocument, relation);
    pointDocument.poiSource = "OSM";
    info.pointDocument = pointDocument;
    info.firstMemberId = members_ids.getFirst();
    info.secondMemberId = members_ids.size() > 1 ? members_ids.get(1) : -1;
    info.memberIds = Collections.synchronizedList(members_ids);

    return List.of(info);
  }

  @Override
  public void preprocessOsmWay(OsmElement.Way way) {
    if (way.hasTag("mtb:name")) {
      String mtbName = way.getString("mtb:name");
      synchronized(mtbName.intern()) {
        if (!Singles.containsKey(mtbName)) {
          var finder = new MinWayIdFinder();
          finder.addWayId(way.id());
          Singles.put(mtbName, finder);
        } else {
          Singles.get(mtbName).addWayId(way.id());
        }
        return;
      }
    }
    if (way.hasTag("waterway") && way.hasTag("name")) {
      String waterwayName = way.getString("name");
      synchronized(waterwayName.intern()) {
        if (!Waterways.containsKey(waterwayName)) {
          var finder = new MinWayIdFinder();
          finder.addWayId(way.id());
          Waterways.put(waterwayName, finder);
        } else {
          Waterways.get(waterwayName).addWayId(way.id());
        }
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
    pointDocument.poiSource = feature.getString("poiSource");
    convertTagsToDocument(pointDocument, feature);
    var point = feature.canBePolygon() ? (Point)feature.centroidIfConvex() : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var docId = pointDocument.poiSource + "_" + feature.getString("identifier");
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};
    
    insertPointToElasticsearch(pointDocument, docId);
    
    var tileFeature = features.geometry("external", point)
        .setAttr("poiId", docId)
        .setAttr("identifier", feature.getString("identifier"))
        .setId(feature.id());
    var languages = feature.hasTag("poiLanguages") ? (ArrayList<String>)feature.getTag("poiLanguages") : new ArrayList<String>();
    setFeaturePropertiesFromPointDocument(tileFeature, pointDocument, languages.toArray(String[]::new));
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
      relation.memberIds.remove(feature.id());

      if (relation.firstMemberId == feature.id()) {
        relation.firstMemberFeature = feature;
      }
      if (relation.secondMemberId == feature.id()) {
        relation.secondMemberFeature = feature;
      }

      if (!relation.memberIds.isEmpty()) {
        continue;
      }
      // All relation members were reached. Add a POI element for line relation
      var point = getFirstPointOfLineRelation(relation);
      var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
      relation.pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};

      insertPointToElasticsearch(relation.pointDocument, "OSM_relation_" + relation.id());

      var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
        .setId(relation.vectorTileFeatureId(config.featureSourceIdMultiplier()));
      setFeaturePropertiesFromPointDocument(tileFeature, relation.pointDocument, this.supportedLanguages);
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
    var single = Singles.get(mtbName);
    synchronized(single) {
      single.lineMerger.add(feature.worldGeometry());
      single.ids.remove(feature.id());

      if (single.minId == feature.id()) {
        single.representingFeature = feature;
      }
      if (!single.ids.isEmpty()) {
        return true;
      }
      var minIdFeature = single.representingFeature;
      var point = GeoUtils.point(((Geometry)single.lineMerger.getMergedLineStrings().iterator().next()).getCoordinate());

      var pointDocument = new PointDocument();
      convertTagsToDocument(pointDocument, feature);
      for (String language : supportedLanguages) {
        CoalesceIntoMap(pointDocument.name, language, minIdFeature.getString("mtb:name:" + language));
      }
      if (minIdFeature.hasTag("mtb:name")) {
        CoalesceIntoMap(pointDocument.name, "default", minIdFeature.getString("mtb:name"));
      }
      pointDocument.poiCategory = "Bicycle";
      pointDocument.poiIcon = "icon-bike";
      pointDocument.poiIconColor = "gray";
      pointDocument.poiSource = "OSM";
      var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
      pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};

      insertPointToElasticsearch(pointDocument, "OSM_way_" + single.minId);
      // This was the last way with the same mtb:name, so we can merge the lines and add the feature
      // Add a POI element for a SingleTrack
      var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
        // Override the feature id with the minimal id of the group
        .setId(minIdFeature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
      setFeaturePropertiesFromPointDocument(tileFeature, pointDocument, this.supportedLanguages);
    }
    return true;
  }

  private boolean processWaterwayFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!feature.hasTag("waterway")) {
      return false;
    }
    if (!feature.hasTag("name")) {
      return false;
    }
    String name = feature.getString("name");
    if (!Waterways.containsKey(name)) {
      return false;
    }
    for (var routeInfo : feature.relationInfo(RelationInfo.class)) {
      RelationInfo relation = routeInfo.relation();
      if (relation.pointDocument.poiIcon == "icon-river") {
        // In case this waterway is part of a relation, we already processed it
        return true;
      }
    }

    var waterway = Waterways.get(name);
    synchronized(waterway) {

      waterway.lineMerger.add(feature.worldGeometry());
      waterway.ids.remove(feature.id());

      if (waterway.minId == feature.id()) {
        waterway.representingFeature = feature;
      }
      if (!waterway.ids.isEmpty()) {
        return true;
      }
      var minIdFeature = waterway.representingFeature;
      var point = GeoUtils.point(((Geometry)waterway.lineMerger.getMergedLineStrings().iterator().next()).getCoordinate());

      var pointDocument = new PointDocument();
      convertTagsToDocument(pointDocument, feature);
      pointDocument.poiCategory = "Water";
      pointDocument.poiIcon = "icon-river";
      pointDocument.poiIconColor = "blue";
      pointDocument.poiSource = "OSM";
      var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
      pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};

      insertPointToElasticsearch(pointDocument, "OSM_way_" + waterway.minId);
      if (!isInterestingPoint(pointDocument)) {
        // Skip adding features without any description or image to tiles
        return true;
      }
      
      var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
        // Override the feature id with the minimal id of the group
        .setId(minIdFeature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
      setFeaturePropertiesFromPointDocument(tileFeature, pointDocument, this.supportedLanguages);

      return true;
    }
  }

  private boolean processHighwayFeautre(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!feature.hasTag("highway") || !feature.hasTag("name")) {
      return false;
    }
    var point = GeoUtils.point(feature.worldGeometry().getCoordinate());
    var pointDocument = new PointDocument();
    convertTagsToDocument(pointDocument, feature);
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
    convertTagsToDocument(pointDocument, feature);
    pointDocument.poiSource = "OSM";
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[]{lngLatPoint.getX(), lngLatPoint.getY()};

    setIconColorCategory(pointDocument, feature);

    if (pointDocument.poiIcon == "icon-search") {
        return false;
    }

    if (feature.getString("place") != null && pointDocument.poiCategory == "Wikipedia" && !feature.isPoint()) {
        return true;
    }

    insertPointToElasticsearch(pointDocument, docId);

    if ((pointDocument.poiIcon == "icon-peak" || pointDocument.poiIcon == "icon-river") && !isInterestingPoint(pointDocument)) {
        return true;
    }

    var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
        .setId(tileId);

    setFeaturePropertiesFromPointDocument(tileFeature, pointDocument, this.supportedLanguages);
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
    if (feature.hasTag("building") && !feature.hasTag("building", "no", "none", "No")) {
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
    if (feature.hasTag("landuse", "forest")) {
      pointDocument.poiIcon = "icon-tree";
      pointDocument.poiIconColor = "#008000";
      pointDocument.poiCategory = "Other";
    }
    
    if (pointDocument.poiIcon == null) {
      return;
    }

    if (pointDocument.poiIcon == "icon-search" && ((feature.getString("wikidata") != null || feature.getString("wikipedia") != null))) {
      pointDocument.poiIconColor = "black";
      pointDocument.poiIcon = "icon-wikipedia-w";
      pointDocument.poiCategory = "Wikipedia";
    }
    for (String language : supportedLanguages) {
      CoalesceIntoMap(pointDocument.name, language, feature.getString("name:" + language));
      CoalesceIntoMap(pointDocument.description, language, feature.getString("description:" + language));
    }
    if (feature.hasTag("name")) {
      CoalesceIntoMap(pointDocument.name, "default", feature.getString("name"));
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
    Geometry polygon;
    try {
      polygon = GeoUtils.worldToLatLonCoords(feature.polygon());
    } catch (GeometryException e) {
      return;
    }
    try {
      var bbox = new BBoxDocument();
      bbox.area = feature.areaMeters();

      bbox.setBBox(polygon);
      for (String lang : supportedLanguages) {
        CoalesceIntoMap(bbox.name, lang, feature.getString("name:" + lang));
      }
      if (feature.hasTag("name")) {
        CoalesceIntoMap(bbox.name, "default", feature.getString("name"));
      }
      esClient.index(i -> i
          .index(this.bboxIndexName)
          .id(sourceFeatureToDocumentId(feature))
          .document(bbox)
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
  private Point getFirstPointOfLineRelation(RelationInfo relation) throws GeometryException {
    if (relation.secondMemberFeature == null) {
      return GeoUtils.point(relation.firstMemberFeature.worldGeometry().getCoordinate());
    }

    var firstMemberGeometry = (LineString) relation.firstMemberFeature.line();
    var firstMemberStartCoordinate = firstMemberGeometry.getCoordinate();
    var firstMemberEndCoordinate = firstMemberGeometry.getCoordinateN(firstMemberGeometry.getNumPoints() - 1);
    
    var secondMemberGeometry = (LineString) relation.secondMemberFeature.line();
    var secondMemberStartCoordinate = secondMemberGeometry.getCoordinate();
    var secondMemberEndCoordinate = secondMemberGeometry.getCoordinateN(secondMemberGeometry.getNumPoints() - 1);

    if (firstMemberStartCoordinate.equals2D(secondMemberStartCoordinate) || firstMemberStartCoordinate.equals2D(secondMemberEndCoordinate)) {
      return GeoUtils.point(firstMemberEndCoordinate);
    }
    if (firstMemberEndCoordinate.equals2D(secondMemberStartCoordinate) || firstMemberEndCoordinate.equals2D(secondMemberEndCoordinate)) {
      return GeoUtils.point(firstMemberStartCoordinate);
    }
    return GeoUtils.point(firstMemberStartCoordinate);
  }

  private boolean isInterestingPoint(PointDocument pointDocument) {
    return !pointDocument.description.isEmpty() ||
      pointDocument.image != null;
  }

  private void setFeaturePropertiesFromPointDocument(Feature tileFeature, PointDocument pointDocument, String[] languages) {
    tileFeature.setAttr("wikidata", pointDocument.wikidata)
        .setAttr("wikimedia_commons", pointDocument.wikimedia_commons)
        .setAttr("image", pointDocument.image)
        .setAttr("poiIcon", pointDocument.poiIcon)
        .setAttr("poiIconColor", pointDocument.poiIconColor)
        .setAttr("poiCategory", pointDocument.poiCategory)
        .setAttr("poiSource", pointDocument.poiSource)
        .setAttr("poiLanguages", String.join(",", languages))
        .setZoomRange(10, 14)
        .setBufferPixels(0);
    for (String lang : supportedLanguages) {
      tileFeature.setAttr("name:" + lang, pointDocument.name.get(lang));
      tileFeature.setAttr("description:" + lang, pointDocument.description.get(lang));
    }
    if (pointDocument.name.containsKey("default")) {
      tileFeature.setAttr("name", pointDocument.name.get("default"));
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
            pointDocument.poiIcon = "icon-leaf";
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

    if ("waterfall".equals(feature.getString("waterway"))) {
        pointDocument.poiIconColor = "blue";
        pointDocument.poiIcon = "icon-waterfall";
        pointDocument.poiCategory = "Water";
        return;
    }

    if ("waterway".equals(feature.getString("type"))) {
        pointDocument.poiIconColor = "blue";
        pointDocument.poiIcon = "icon-river";
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
            case "artwork":
                pointDocument.poiIconColor = "#ffb800";
                pointDocument.poiIcon = "icon-artwork";
                pointDocument.poiCategory = "Other";
                return;
          case "alpine_hut":
                pointDocument.poiIconColor = "#734a08";
                pointDocument.poiIcon = "icon-home";
                pointDocument.poiCategory = "Camping";
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
