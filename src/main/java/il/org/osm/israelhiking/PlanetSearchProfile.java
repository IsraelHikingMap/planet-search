package il.org.osm.israelhiking;

import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.RELATION;
import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.WAY;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;

import il.org.osm.israelhiking.ElasticsearchHelper.ElasticRunContext;

public class PlanetSearchProfile implements Profile {
  private static final Logger LOGGER = LoggerFactory.getLogger(PlanetSearchProfile.class);

  private PlanetilerConfig config;
  private ElasticRunContext context;

  /**
   * Containment near a border is fuzzy anyway; ~0.0005° ≈ 50 m trims the polygons
   * hard. This is the ceiling — it is what the big admin boundaries, which hold
   * the bulk of the vertices, are actually simplified by.
   */
  private static final double CONTAINER_SIMPLIFY_DEGREES = 0.0005;

  /**
   * The share of a container's own extent the simplification may move its
   * outline by, capped at {@link #CONTAINER_SIMPLIFY_DEGREES}. A flat 50 m is a
   * rounding error on a country and more than a tenth of the radius of a village
   * — enough to cut a whole lobe off a small settlement, leaving the streets in
   * it attributed to the regional council instead. Tying the tolerance to the
   * polygon keeps the error proportional, and costs little: the polygons this
   * gives a finer tolerance to are two thirds of the containers but a sixth of
   * the stored vertices.
   */
  private static final double CONTAINER_SIMPLIFY_RELATIVE = 0.01;

  public static final String POINTS_LAYER_NAME = "global_points";

  /**
   * Values that must never become a container
   */
  private static final Set<String> NON_CONTAINER_PLACES = Set.of(
      "suburb", "neighbourhood", "quarter", "city_block", "borough",
      "square", "locality", "islet", "farm", "isolated_dwelling", "plot");

  private static final Map<String, MinWayIdFinder> Singles = new ConcurrentHashMap<>();
  private static final Map<String, MinWayIdFinder> NamedHighways = new ConcurrentHashMap<>();
  private static final Map<String, MinWayIdFinder> Waterways = new ConcurrentHashMap<>();
  private final PlaceHelper placeHelper = new PlaceHelper();

  public PlanetSearchProfile(PlanetilerConfig config, ElasticRunContext context) {
    this.config = config;
    this.context = context;
  }

  /*
   * The processing happens in 3 steps:
   * 1. On the first pass through the input file, store relevant information from
   * applicable OSM route relations and ways with mtb:name tag.
   * 2. On the second pass, emit points for relation and mtb:name ways. Emit a
   * point by merging all the ways and using the first point of the merged
   * linestring.
   * 
   * Step 1)
   *
   * Planetiler processes the .osm.pbf input file in two passes. The first pass
   * stores node locations, and invokes
   * preprocessOsmRelation for reach relation and stores information the profile
   * needs during the second pass when we
   * emit map feature for ways contained in that relation.
   * 
   * Step 2)
   *
   * On the second pass through the input .osm.pbf file, for each way in a
   * relation that we stored data about, emit a
   * point with attributes derived from the relation as well as for ways with
   * mtb:name tag.
   */

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    placeHelper.recordRelationIfNeeded(relation);
    // If this is a "route" relation ...
    if (relation.hasTag("state", "proposed")) {
      return null;
    }
    var category = OsmFeatureClassifier.classify(relation);
    if (!"icon-river".equals(category.icon) &&
        !"Bicycle".equals(category.poiCategory) &&
        !"Hiking".equals(category.poiCategory) &&
        !"4x4".equals(category.poiCategory)) {
      return null;
    }
    // then store a RouteRelationInfo instance with tags we'll need later
    var waysMemberIds = relation.members()
        .stream()
        .filter(member -> member.type() == WAY)
        .mapToLong(OsmElement.Relation.Member::ref)
        .boxed()
        .collect(Collectors.toList());

    var relationMemberIds = relation.members()
        .stream()
        .filter(member -> member.type() == RELATION)
        .mapToLong(OsmElement.Relation.Member::ref)
        .boxed()
        .collect(Collectors.toList());

    if (waysMemberIds.isEmpty() && relationMemberIds.isEmpty()) {
      return null;
    }
    var info = new RelationInfo(relation.id());

    info.pointDocument = this.context.documentFactory().buildRouteRelationDocument(relation, category);
    if (waysMemberIds.size() > 0) {
      info.firstMemberId = waysMemberIds.getFirst();
      info.secondMemberId = waysMemberIds.size() > 1 ? waysMemberIds.get(1) : -1;
    } else if (relationMemberIds.size() > 0) {
      info.firstMemberId = relationMemberIds.getFirst();
      info.secondMemberId = relationMemberIds.size() > 1 ? relationMemberIds.get(1) : -1;
    }

    info.waysMemberIds = Collections.synchronizedList(waysMemberIds);
    info.RelationMemberIds = Collections.synchronizedList(relationMemberIds);
    info.isSuperRelation = info.RelationMemberIds.size() > 0;
    return List.of(info);
  }

  @Override
  public void preprocessOsmWay(OsmElement.Way way) {
    if (way.hasTag("mtb:name")) {
      String mtbName = way.getString("mtb:name");
      synchronized (mtbName.intern()) {
        if (!Singles.containsKey(mtbName)) {
          var finder = new MinWayIdFinder();
          finder.ids.add(way.id());
          Singles.put(mtbName, finder);
        } else {
          Singles.get(mtbName).ids.add((way.id()));
        }
        return;
      }
    }
    if (way.hasTag("waterway") && way.hasTag("name")) {
      String waterwayName = way.getString("name");
      synchronized (waterwayName.intern()) {
        if (!Waterways.containsKey(waterwayName)) {
          var finder = new MinWayIdFinder();
          finder.ids.add((way.id()));
          Waterways.put(waterwayName, finder);
        } else {
          Waterways.get(waterwayName).ids.add((way.id()));
        }
      }
      return;
    }

    if (way.hasTag("highway", "track", "path", "footway", "cycleway") && way.hasTag("name")) {
      String highwayName = way.getString("name");
      synchronized (highwayName.intern()) {
        if (!NamedHighways.containsKey(highwayName)) {
          var finder = new MinWayIdFinder();
          finder.ids.add((way.id()));
          NamedHighways.put(highwayName, finder);
        } else {
          NamedHighways.get(highwayName).ids.add((way.id()));
        }
      }
      return;
    }
  }

  @Override
  public void processFeature(SourceFeature feature, FeatureCollector features) {
    try {
      if (feature.getSource() == "external") {
        processExternalFeautre(feature, features);
        return;
      }
      if (isBBoxFeature(feature, this.context.supportedLanguages())) {
        insertBboxToElasticsearch(feature, this.context.supportedLanguages());
      }
      processOsmRelationFeature(feature, features);
      if (processMtbNameFeature(feature, features))
        return;
      if (processWaterwayFeature(feature, features))
        return;
      if (processStreetFeature(feature))
        return;
      if (processHighwayFeautre(feature, features))
        return;
      if (processPlaceFeature(feature, features))
        return;
      if (processOtherSourceFeature(feature, features))
        return;
      addNonIconFeaturesToElasricseach(feature);
    } catch (GeometryException e) {
      // ignore bad geometries
    }
  }

  private void processExternalFeautre(SourceFeature feature, FeatureCollector features) throws GeometryException {
    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    var pointDocument = this.context.documentFactory().buildExternalDocument(feature,
        lngLatPoint.getX(), lngLatPoint.getY());
    var docId = pointDocument.poiSource + "_" + feature.getString("identifier");
    insertPointToElasticsearch(pointDocument, docId);

    var tileFeature = features.geometry("external", point)
        .setAttr("poiId", docId)
        .setAttr("identifier", feature.getString("identifier"))
        .setAttr("poiUserId", feature.getString("poiUserId"))
        .setId(feature.id());
    setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
  }

  private void processOsmRelationFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    // get all the RouteRelationInfo instances we returned from
    // preprocessOsmRelation that this way belongs to, including super relations.
    for (var routeInfo : feature.relationInfo(RelationInfo.class, true)) {
      RelationInfo relation = routeInfo.relation();
      synchronized (relation) {
        if (relation.firstMemberId == feature.id()) {
          relation.firstMemberFeature = feature;
        }
        if (relation.secondMemberId == feature.id()) {
          relation.secondMemberFeature = feature;
        }
        if (relation.waysMemberIds.remove(feature.id())) {
          relation.length += feature.lengthMeters();
        }
      }
    }

    handleSuperRelationMembersUpdate(feature);

    for (var routeInfo : feature.relationInfo(RelationInfo.class, true)) {
      RelationInfo relation = routeInfo.relation();
      if (!relation.waysMemberIds.isEmpty() || !relation.RelationMemberIds.isEmpty()) {
        continue;
      }

      if (relation.pointDocument.name.isEmpty()) {
        continue;
      }
      // All relation members were reached. Add a POI element for line relation
      var point = getFirstPointOfLineRelation(relation.firstMemberFeature, relation.secondMemberFeature);
      var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
      relation.pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };
      relation.pointDocument.poiLength = relation.length;
      this.context.containerIndex().enrich(relation.pointDocument, false);
      insertPointToElasticsearch(relation.pointDocument, "OSM_relation_" + relation.id());

      var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
          .setId(relation.vectorTileFeatureId(config.featureSourceIdMultiplier()));
      setFeaturePropertiesFromPointDocument(tileFeature, relation.pointDocument);
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
    synchronized (single) {
      single.ids.remove(feature.id());
      if (feature.canBeLine()) {
        single.features.add(feature);
      }

      if (!single.ids.isEmpty()) {
        return true;
      }

      for (var mergedFeature : single.getMergedFeatures()) {
        var minIdFeature = mergedFeature.representingFeature;

        var firstLine = mergedFeature.geometry;
        var point = GeoUtils.point(((Geometry) firstLine).getCoordinate());
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        var pointDocument = this.context.documentFactory().buildMtbDocument(minIdFeature,
            lngLatPoint.getX(), lngLatPoint.getY(), mergedFeature.length);
        insertPointToElasticsearch(pointDocument, "OSM_way_" + mergedFeature.minId);
        // This was the last way with the same mtb:name, so we can merge the lines and
        // add the feature
        // Add a POI element for a SingleTrack
        var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
            // Override the feature id with the minimal id of the group
            .setId(minIdFeature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
        setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
      }
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
    synchronized (waterway) {

      waterway.ids.remove(feature.id());
      if (feature.canBeLine()) {
        waterway.features.add(feature);
      }
      if (!waterway.ids.isEmpty()) {
        return true;
      }
      for (var mergedFeature : waterway.getMergedFeatures()) {
        var minIdFeature = mergedFeature.representingFeature;

        var firstLine = mergedFeature.geometry;
        var point = GeoUtils.point(((Geometry) firstLine).getCoordinate());
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        var pointDocument = this.context.documentFactory().buildWaterwayDocument(minIdFeature,
            lngLatPoint.getX(), lngLatPoint.getY(), mergedFeature.length);
        insertPointToElasticsearch(pointDocument, "OSM_way_" + mergedFeature.minId);
        if (!isInterestingPoint(pointDocument)) {
          // Skip adding features without any description or image to tiles
          continue;
        }

        var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
            // Override the feature id with the minimal id of the group
            .setId(minIdFeature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
        setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
      }
      return true;
    }
  }

  private boolean processHighwayFeautre(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!feature.hasTag("highway")) {
      return false;
    }
    if (!feature.hasTag("name")) {
      // Highways without a name should not be included in the search or POI layer.
      return true;
    }
    if (feature.isPoint()) {
      // We don't want to process highway nodes (bus stops, etc.) here.
      return false;
    }
    if (!feature.hasTag("highway", "track", "path", "footway", "cycleway")) {
      return true;
    }

    String name = feature.getString("name");
    if (!NamedHighways.containsKey(name)) {
      return true;
    }

    var highway = NamedHighways.get(name);
    synchronized (highway) {

      highway.ids.remove(feature.id());
      if (feature.canBeLine()) {
        highway.features.add(feature);
      }

      if (!highway.ids.isEmpty()) {
        return true;
      }

      for (var mergedFeature : highway.getMergedFeatures()) {
        var minIdFeature = mergedFeature.representingFeature;
        var point = GeoUtils.point((mergedFeature.geometry.getCoordinate()));
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        var pointDocument = this.context.documentFactory().buildNamedHighwayDocument(minIdFeature,
            lngLatPoint.getX(), lngLatPoint.getY(), mergedFeature.length);
        insertPointToElasticsearch(pointDocument, "OSM_way_" + mergedFeature.minId);

        if (pointDocument.poiIcon == "icon-hike" ||
            pointDocument.poiIcon == "icon-bike" ||
            pointDocument.poiIcon == "icon-four-by-four") {
          continue;
        }
        // This is a highway with a name, but it's not just a highway as it has a
        // different icon, so adding it to the list of points.
        var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
            // Override the feature id with the minimal id of the group
            .setId(minIdFeature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
        setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
      }

      return true;
    }
  }

  /**
   * Records a named street line for the merge that runs after the input pass:
   * the street's segments are keyed by name and enclosing container so its many
   * ways collapse into one search document under its minimal way id. Streets are
   * search only, so this never emits a tile feature.
   *
   * This runs on every named road way on the planet and all but one segment per
   * street is then thrown away, so it does the least it can: it records the way
   * id, the point and the settlement's name, and builds no document at all.
   * {@link StreetIndex#flush} builds the document for the segment that wins,
   * through {@link #buildStreetDocument}.
   */
  private boolean processStreetFeature(SourceFeature feature) throws GeometryException {
    if (!StreetIndex.isStreet(feature) || !feature.canBeLine()) {
      return false;
    }
    var startPoint = feature.line().getCoordinate();
    double lng = GeoUtils.getWorldLon(startPoint.getX());
    double lat = GeoUtils.getWorldLat(startPoint.getY());
    this.context.streetIndex().add(feature.id(), feature.getString("name"),
        this.context.containerIndex().tightestContainerName(lat, lng), lng, lat);
    return true;
  }

  /**
   * Places get their own flow, so a place is searchable by name even when it has
   * no dedicated place node (common in Israel), while a place with several
   * representations shows up only once. A representation — node or polygon — is
   * dropped when a better-ranked polygon of the same place already carries its
   * representative point (the node, or the polygon's convex centre): either the
   * polygon encloses that point or its centre sits within a few km of it, per the
   * container index. Whatever is skipped here still serves as a bbox container,
   * indexed separately by {@link #insertBboxToElasticsearch}.
   */
  private boolean processPlaceFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    String place = feature.getString("place");
    if (place == null || place.isBlank()) {
      return false;
    }
    if (feature.isPoint()) {
      // A place node may be a relation's anchor; remember where it is for that
      // relation.
      var worldCoordinate = feature.worldGeometry().getCoordinate();
      placeHelper.captureMemberNode(feature.id(), worldCoordinate.getX(), worldCoordinate.getY());
    }
    if (!OsmNames.hasSearchableName(feature, this.context.supportedLanguages())) {
      // Nothing to search on; leave nameless places to the generic flow.
      return false;
    }

    var anchor = placeHelper.getLabelNodeWorldLocation(feature);
    Point point;
    if (anchor != null) {
      point = GeoUtils.point(anchor[0], anchor[1]);
    } else {
      point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
          : GeoUtils.point(feature.worldGeometry().getCoordinate());
    }
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    var isCoveredByBetterPlace = this.context.containerIndex().coveredByBetterPlace(
        lngLatPoint.getY(),
        lngLatPoint.getX(),
        PlaceHelper.getPlaceNames(feature, this.context.supportedLanguages()), feature.getString("wikidata"),
        PlaceHelper.calculatePlaceRank(feature), feature.id());

    if (isCoveredByBetterPlace) {
      // A stronger representation of this same place already carries this point.
      return true;
    }

    var pointDocument = this.context.documentFactory().buildPlaceDocument(feature,
        lngLatPoint.getX(), lngLatPoint.getY(), feature.canBePolygon() ? feature.areaMeters() : null);
    insertPointToElasticsearch(pointDocument, sourceFeatureToDocumentId(feature));

    var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
        .setId(feature.vectorTileFeatureId(config.featureSourceIdMultiplier()));
    setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
    return true;
  }

  private boolean processOtherSourceFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    if (!OsmNames.hasSearchableName(feature, this.context.supportedLanguages()) &&
        !feature.hasTag("wikidata") &&
        !feature.hasTag("image") &&
        !feature.hasTag("description") &&
        !feature.hasTag("ref:IL:inature")) {
      return false;
    }

    var tileId = feature.vectorTileFeatureId(config.featureSourceIdMultiplier());
    var docId = sourceFeatureToDocumentId(feature);
    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());

    var category = OsmFeatureClassifier.classify(feature);
    if (category.icon == "icon-search") {
      return false;
    }

    if (feature.getString("place") != null && !feature.isPoint()) {
      return true;
    }

    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    var pointDocument = this.context.documentFactory().buildPointOfInterestDocument(feature,
        lngLatPoint.getX(), lngLatPoint.getY(), category, feature.canBePolygon() ? feature.areaMeters() : null);
    insertPointToElasticsearch(pointDocument, docId);

    if ((pointDocument.poiIcon == "icon-peak" || pointDocument.poiIcon == "icon-river")
        && !isInterestingPoint(pointDocument)) {
      return true;
    }

    var tileFeature = features.geometry(POINTS_LAYER_NAME, point)
        .setId(tileId);

    setFeaturePropertiesFromPointDocument(tileFeature, pointDocument);
    return true;
  }

  private void addNonIconFeaturesToElasricseach(SourceFeature feature) throws GeometryException {
    if (!OsmNames.hasSearchableName(feature, this.context.supportedLanguages())) {
      return;
    }
    var category = OsmFeatureClassifier.classifyNonIcon(feature);
    if (category == null) {
      return;
    }
    var docId = sourceFeatureToDocumentId(feature);
    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    var pointDocument = this.context.documentFactory().buildNonIconDocument(feature,
        lngLatPoint.getX(), lngLatPoint.getY(), category);
    insertPointToElasticsearch(pointDocument, docId);
  }

  private void insertPointToElasticsearch(PointDocument pointDocument, String docId) {
    this.context.bulkListener().add(BulkOperation.of(op -> op
        .index(idx -> idx
            .index(this.context.pointsIndexTarget())
            .id(docId)
            .document(pointDocument))));
  }

  private void insertBboxToElasticsearch(SourceFeature feature, String[] supportedLanguages) {
    var documentId = sourceFeatureToDocumentId(feature);
    Geometry polygon;
    try {
      polygon = repairPolygonIfNeeded(GeoUtils.worldToLatLonCoords(feature.polygon()));
    } catch (GeometryException e) {
      return;
    }
    if (polygon == null) {
      return;
    }
    Geometry simplified = simplifyContainer(polygon);
    try {
      var bbox = new BBoxDocument();
      bbox.area = feature.areaMeters();
      bbox.adminLevel = feature.hasTag("admin_level") ? (int) feature.getLong("admin_level") : 0;
      bbox.wikidata = feature.getString("wikidata");
      bbox.placeRank = PlaceHelper.calculatePlaceRank(feature).ordinal();
      bbox.id = feature.id();
      var lngLatCenterPoint = GeoUtils.worldToLatLonCoords(feature.centroid()).getCoordinate();
      bbox.center = new double[] { lngLatCenterPoint.getX(), lngLatCenterPoint.getY() };
      bbox.setBBox(simplified);
      for (String lang : supportedLanguages) {
        PointDocumentFactory.CoalesceIntoMap(bbox.name, lang, feature.getString("name:" + lang));
      }
      if (feature.hasTag("name")) {
        PointDocumentFactory.CoalesceIntoMap(bbox.name, "default", feature.getString("name"));
      }
      this.context.bulkListener().add(BulkOperation.of(op -> op
          .index(idx -> idx
              .index(this.context.bboxIndexTarget())
              .id(documentId)
              .document(bbox))));
    } catch (Exception e) {
      this.context.bulkListener().recordFailure(this.context.bboxIndexTarget());
      LOGGER.warn("Failed to index the bounding box of {}: {}", documentId, e.getMessage());
    }
  }

  /**
   * Containment near a border is fuzzy anyway; simplifying keeps geometry cheap
   * to read and write. The tolerance scales with the polygon, so a village's
   * outline is not moved as far as a country's — see
   * {@link #CONTAINER_SIMPLIFY_RELATIVE}. It only ever moves inwards or along
   * the border, never deliberately outwards: adjacent towns share a border and
   * the tightest container wins, so a polygon grown past its border would take
   * the strip along it from its neighbour.
   */
  private static Geometry simplifyContainer(Geometry polygon) {
    var envelope = polygon.getEnvelopeInternal();
    double extent = Math.sqrt(envelope.getWidth() * envelope.getHeight());
    double tolerance = Math.min(CONTAINER_SIMPLIFY_DEGREES, extent * CONTAINER_SIMPLIFY_RELATIVE);
    try {
      return TopologyPreservingSimplifier.simplify(polygon, tolerance);
    } catch (RuntimeException e) {
      return polygon;
    }
  }

  /**
   * Repairs the polygon before indexing it. OSM boundaries are sometimes self
   * intersecting,
   * and Elasticsearch rejects such a polygon
   * 
   * @return a polygon Elasticsearch can index, or null when even that failed.
   */
  private static Geometry repairPolygonIfNeeded(Geometry polygon) {
    if (polygon.isValid()) {
      return polygon;
    }
    var fixed = GeometryFixer.fix(polygon);
    if (fixed.isEmpty() || !fixed.isValid() || !(fixed instanceof Polygonal)) {
      return null;
    }
    return fixed;
  }

  /**
   * Get the first point of the trail relation by checking some heuristics related
   * to the relation's first member
   * 
   * @param mergedLines - the merged lines helper
   * @return the first point of the trail relation
   * @throws GeometryException
   */
  private Point getFirstPointOfLineRelation(SourceFeature firstMemberFeature, SourceFeature secondMemberFeature)
      throws GeometryException {
    if (secondMemberFeature == null) {
      return GeoUtils.point(firstMemberFeature.worldGeometry().getCoordinate());
    }

    var firstMemberGeometry = (LineString) firstMemberFeature.line();
    var firstMemberStartCoordinate = firstMemberGeometry.getCoordinate();
    var firstMemberEndCoordinate = firstMemberGeometry.getCoordinateN(firstMemberGeometry.getNumPoints() - 1);
    var secondMemberGeometry = (LineString) secondMemberFeature.line();
    var secondMemberStartCoordinate = secondMemberGeometry.getCoordinate();
    var secondMemberEndCoordinate = secondMemberGeometry.getCoordinateN(secondMemberGeometry.getNumPoints() - 1);

    if (firstMemberStartCoordinate.equals2D(secondMemberStartCoordinate)
        || firstMemberStartCoordinate.equals2D(secondMemberEndCoordinate)) {
      return GeoUtils.point(firstMemberEndCoordinate);
    }
    if (firstMemberEndCoordinate.equals2D(secondMemberStartCoordinate)
        || firstMemberEndCoordinate.equals2D(secondMemberEndCoordinate)) {
      return GeoUtils.point(firstMemberStartCoordinate);
    }
    return GeoUtils.point(firstMemberStartCoordinate);
  }

  /**
   * This method removes relation members that are part of super relations and
   * have completed the ways processing.
   * This is done by checking for each new way that is being processed if it
   * completes a relation,
   * and remove that relation from the list of parent relations, this way at some
   * point all the ways and relations are empty
   * and it means we can continue processing them to add them to the database and
   * tiles.
   * It also keeps track of the first and second member features in case they are
   * needed to determine the first point of the relation.
   * 
   * @param feature
   */
  private void handleSuperRelationMembersUpdate(SourceFeature feature) {
    var removedElement = false;
    do {
      removedElement = false;
      for (var routeInfo : feature.relationInfo(RelationInfo.class, true)) {
        RelationInfo relation = routeInfo.relation();
        if (!relation.waysMemberIds.isEmpty() || !relation.RelationMemberIds.isEmpty()) {
          continue;
        }
        for (var superRouteInfo : feature.relationInfo(RelationInfo.class, true)) {
          RelationInfo superRelation = superRouteInfo.relation();
          if (!superRelation.isSuperRelation) {
            continue;
          }
          synchronized (superRelation) {
            if (superRelation.RelationMemberIds.remove(relation.id())) {
              superRelation.length += relation.length;
              removedElement = true;
              if (superRelation.firstMemberId == relation.id()) {
                superRelation.firstMemberFeature = relation.firstMemberFeature;
                superRelation.secondMemberFeature = relation.secondMemberFeature;
              }
            }
          }
        }
      }
    } while (removedElement);
  }

  private boolean isInterestingPoint(PointDocument pointDocument) {
    return !pointDocument.description.isEmpty() ||
        pointDocument.image != null;
  }

  private void setFeaturePropertiesFromPointDocument(Feature tileFeature, PointDocument pointDocument) {
    tileFeature.setAttr("wikidata", pointDocument.wikidata)
        .setAttr("wikimedia_commons", pointDocument.wikimedia_commons)
        .setAttr("image", pointDocument.image)
        .setAttr("website", pointDocument.website)
        .setAttr("poiIcon", pointDocument.poiIcon)
        .setAttr("poiIconColor", pointDocument.poiIconColor)
        .setAttr("poiCategory", pointDocument.poiCategory)
        .setAttr("poiSource", pointDocument.poiSource)
        .setAttr("poiLength", pointDocument.poiLength)
        .setAttr("poiDifficulty", pointDocument.poiDifficulty)
        .setZoomRange(8, 14)
        .setBufferPixels(0);
    for (String lang : this.context.supportedLanguages()) {
      tileFeature.setAttr("name:" + lang, pointDocument.name.get(lang));
      tileFeature.setAttr("description:" + lang, pointDocument.description.get(lang));
    }
    if (pointDocument.name.containsKey("default")) {
      tileFeature.setAttr("name", pointDocument.name.get("default"));
    }
    if (pointDocument.description.containsKey("default")) {
      tileFeature.setAttr("description", pointDocument.description.get("default"));
    }
  }

  private boolean isBBoxFeature(SourceFeature feature, String[] supportedLanguages) {
    if (!feature.canBePolygon()) {
      return false;
    }
    if (!OsmNames.hasSearchableName(feature, supportedLanguages)) {
      return false;
    }
    var isFeatureADecentCity = feature.hasTag("boundary", "administrative") &&
        feature.hasTag("admin_level") &&
        feature.getLong("admin_level") > 0 &&
        feature.getLong("admin_level") <= 8;
    if (isFeatureADecentCity) {
      return true;
    }
    if (feature.hasTag("place") && !NON_CONTAINER_PLACES.contains(feature.getString("place"))) {
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
            : "relation_")
        + feature.id();
  }

  /*
   * Hooks to override metadata values in the output mbtiles file. Only name is
   * required, the rest are optional. Bounds,
   * center, minzoom, maxzoom are set automatically based on input data and
   * planetiler config.
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
   * Any time you use OpenStreetMap data, you must ensure clients display the
   * following copyright. Most clients will
   * display this automatically if you populate it in the attribution metadata in
   * the mbtiles file:
   */
  @Override
  public String attribution() {
    return """
        <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
        """.trim();
  }
}
