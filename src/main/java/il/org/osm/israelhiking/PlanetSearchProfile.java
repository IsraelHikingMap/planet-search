package il.org.osm.israelhiking;

import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.RELATION;
import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.WAY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.math.NumberUtils;
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
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;

import il.org.osm.israelhiking.ContainerIndex.ContainerRecord;
import il.org.osm.israelhiking.ElasticsearchHelper.ElasticRunContext;

public class PlanetSearchProfile implements Profile {
  private static final Logger LOGGER = LoggerFactory.getLogger(PlanetSearchProfile.class);

  private PlanetilerConfig config;
  private ElasticRunContext context;

  /**
   * Containment near a border is fuzzy anyway; ~0.0005° ≈ 50 m trims the polygons
   * hard.
   */
  private static final double CONTAINER_SIMPLIFY_DEGREES = 0.0005;

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

  /** Ranks a place's node / way / relation forms so each place is indexed exactly once. */
  private final PlaceIndex placeIndex = new PlaceIndex();

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

  /**
   * Collects all the alternative names of a feature for a single language and
   * stores them under that language in the alt_names map.
   */
  private static void AddAlternativeNames(PointDocument pointDocument, WithTags feature, String language) {
    var alternativeNames = OsmNames.alternativeNames(feature, language);
    if (alternativeNames.isEmpty()) {
      return;
    }
    if (pointDocument.alt_names == null) {
      pointDocument.alt_names = new HashMap<String, List<String>>();
    }
    pointDocument.alt_names.put(language, alternativeNames);
  }

  private void convertTagsToDocument(PointDocument pointDocument, WithTags feature) {
    for (String language : this.context.supportedLanguages()) {
      CoalesceIntoMap(pointDocument.name, language, feature.getString("name:" + language));
      CoalesceIntoMap(pointDocument.description, language, feature.getString("description:" + language));
      AddAlternativeNames(pointDocument, feature, language);
    }
    if (feature.hasTag("name")) {
      CoalesceIntoMap(pointDocument.name, "default", feature.getString("name"));
    }
    if (feature.hasTag("description")) {
      CoalesceIntoMap(pointDocument.description, "default", feature.getString("description"));
    }
    AddAlternativeNames(pointDocument, feature, "default");
    setDifficulty(pointDocument, feature);
    pointDocument.wikidata = feature.getString("wikidata");
    pointDocument.image = feature.getString("image");
    pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
    pointDocument.website = feature.getString("website");
    if (feature.hasTag("intermittent", "yes")) {
      pointDocument.intermittent = true;
    }
    setProminence(pointDocument, feature);
    PlaceIndex.estimatePopulation(feature).ifPresent(population -> pointDocument.population = population);
  }

  private void setProminence(PointDocument pointDocument, WithTags feature) {
    long qrankRaw = this.context.qrankLookup().qrankFor(pointDocument.wikidata);
    double ele = OsmNumberParser.parseElevation(feature.getString("ele")).orElse(Double.NaN);
    boolean hasImage = pointDocument.image != null || pointDocument.wikimedia_commons != null;
    boolean hasWebsite = pointDocument.website != null;
    boolean hasWikidata = pointDocument.wikidata != null;

    pointDocument.poiProminence = ProminenceCalculator.compute(
        OsmFeatureClassifier.classify(feature), ele, hasImage, hasWebsite, hasWikidata, qrankRaw);
  }

  private void setDifficulty(PointDocument pointDocument, WithTags feature) {
    if (feature.hasTag("sac_scale")) {
      switch (feature.getString("sac_scale")) {
        case "none":
          pointDocument.poiDifficulty = "Easy";
          break;
        case "T1":
          pointDocument.poiDifficulty = "Moderate";
          break;
        case "T2":
          pointDocument.poiDifficulty = "Hard";
          break;
        case "T3":
        case "T4":
        case "T5":
        case "T6":
          pointDocument.poiDifficulty = "Very Hard";
          break;
      }
    } else if (feature.hasTag("mtb:scale")) {
      switch (feature.getString("mtb:scale")) {
        case "0":
          pointDocument.poiDifficulty = "Easy";
          break;
        case "1":
          pointDocument.poiDifficulty = "Moderate";
          break;
        case "2":
          pointDocument.poiDifficulty = "Hard";
          break;
        case "3":
        case "4":
        case "5":
        case "6":
          pointDocument.poiDifficulty = "Very Hard";
          break;
      }
    } else if (feature.hasTag("tracktype")) {
      switch (feature.getString("tracktype")) {
        case "grade1":
        case "grade2":
          pointDocument.poiDifficulty = "Easy";
          break;
        case "grade3":
          pointDocument.poiDifficulty = "Moderate";
          break;
        case "grade4":
          pointDocument.poiDifficulty = "Hard";
          break;
        case "grade5":
          pointDocument.poiDifficulty = "Very Hard";
          break;
      }
    }
  }

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    // If this is a "route" relation ...
    if (relation.hasTag("state", "proposed")) {
      return null;
    }
    placeIndex.recordRelation(relation);
    var pointDocument = new PointDocument();
    setIconColorCategory(pointDocument, relation);

    if (!"icon-river".equals(pointDocument.poiIcon) &&
        !"Bicycle".equals(pointDocument.poiCategory) &&
        !"Hiking".equals(pointDocument.poiCategory) &&
        !"4x4".equals(pointDocument.poiCategory)) {
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

    convertTagsToDocument(pointDocument, relation);

    pointDocument.poiSource = "OSM";
    info.pointDocument = pointDocument;
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
  public void preprocessOsmNode(OsmElement.Node node) {
    placeIndex.recordNode(node, this.context.supportedLanguages());
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
    var pointDocument = new PointDocument();
    pointDocument.poiIcon = feature.getString("poiIcon");
    pointDocument.poiIconColor = feature.getString("poiIconColor");
    pointDocument.poiCategory = feature.getString("poiCategory");
    pointDocument.poiSource = feature.getString("poiSource");
    pointDocument.poiDifficulty = feature.getString("poiDifficulty");
    pointDocument.poiLength = NumberUtils.toDouble(feature.getString("poiLength"), 0.0);
    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var docId = pointDocument.poiSource + "_" + feature.getString("identifier");
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

    convertTagsToDocument(pointDocument, feature);
    enrichWithContainers(pointDocument, false);
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
        if (relation.waysMemberIds.remove(feature.id())) {
          relation.length += feature.lengthMeters();
        }
        if (relation.firstMemberId == feature.id()) {
          relation.firstMemberFeature = feature;
        }
        if (relation.secondMemberId == feature.id()) {
          relation.secondMemberFeature = feature;
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
      enrichWithContainers(relation.pointDocument, false);
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
      single.features.add(feature);
      single.ids.remove(feature.id());

      if (!single.ids.isEmpty()) {
        return true;
      }

      for (var mergedFeature : single.getMergedFeatures()) {
        var minIdFeature = mergedFeature.representingFeature;

        var pointDocument = new PointDocument();
        pointDocument.poiCategory = "Bicycle";
        pointDocument.poiIcon = "icon-bike";
        pointDocument.poiIconColor = "gray";
        pointDocument.poiSource = "OSM";
        pointDocument.poiLength = mergedFeature.length;

        var firstLine = mergedFeature.geometry;
        var point = GeoUtils.point(((Geometry) firstLine).getCoordinate());
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

        convertTagsToDocument(pointDocument, minIdFeature);
        for (String language : this.context.supportedLanguages()) {
          CoalesceIntoMap(pointDocument.name, language, minIdFeature.getString("mtb:name:" + language));
        }
        if (minIdFeature.hasTag("mtb:name")) {
          CoalesceIntoMap(pointDocument.name, "default", minIdFeature.getString("mtb:name"));
        }
        enrichWithContainers(pointDocument, false);
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

      waterway.features.add(feature);
      waterway.ids.remove(feature.id());
      if (!waterway.ids.isEmpty()) {
        return true;
      }
      for (var mergedFeature : waterway.getMergedFeatures()) {
        var minIdFeature = mergedFeature.representingFeature;

        var pointDocument = new PointDocument();
        pointDocument.poiCategory = "Water";
        pointDocument.poiIcon = "icon-river";
        pointDocument.poiIconColor = "#1e80e3";
        pointDocument.poiSource = "OSM";
        pointDocument.poiLength = mergedFeature.length;

        var firstLine = mergedFeature.geometry;
        var point = GeoUtils.point(((Geometry) firstLine).getCoordinate());
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

        convertTagsToDocument(pointDocument, minIdFeature);
        enrichWithContainers(pointDocument, false);
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

      highway.features.add(feature);
      highway.ids.remove(feature.id());

      if (!highway.ids.isEmpty()) {
        return true;
      }

      for (var mergedFeature : highway.getMergedFeatures()) {
        var minIdFeature = mergedFeature.representingFeature;
        var pointDocument = new PointDocument();
        setIconColorCategory(pointDocument, minIdFeature);
        pointDocument.poiSource = "OSM";
        pointDocument.poiLength = mergedFeature.length;

        var point = GeoUtils.point((mergedFeature.geometry.getCoordinate()));
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };
        convertTagsToDocument(pointDocument, minIdFeature);
        enrichWithContainers(pointDocument, false);
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
   * Places get their own flow, so a place is searchable by name even when it has
   * no dedicated place node (common in Israel), while a place with several
   * representations shows up only once. {@link PlaceIndex} decides which
   * representation to keep (relation &gt; node &gt; way) and which tags to index;
   * whatever is skipped here still serves as a bbox container, indexed separately
   * by {@link #insertBboxToElasticsearch}.
   */
  private boolean processPlaceFeature(SourceFeature feature, FeatureCollector features) throws GeometryException {
    String place = feature.getString("place");
    if (place == null || place.isBlank()) {
      return false;
    }
    if (!OsmNames.hasSearchableName(feature, this.context.supportedLanguages())) {
      // Nothing to search on; leave nameless places to the generic flow.
      return false;
    }
    var kind = PlaceIndex.kindOf(feature);
    if (!placeIndex.shouldIndex(kind, feature)) {
      // Another representation of this place carries the searchable point.
      return true;
    }
    WithTags tags = placeIndex.tagsToIndex(kind, feature);

    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var pointDocument = new PointDocument();
    if (feature.canBePolygon()) {
      pointDocument.poiAreaNormalized = normalizeArea(feature.areaMeters());
    }
    pointDocument.poiSource = "OSM";
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };
    setIconColorCategory(pointDocument, tags);
    convertTagsToDocument(pointDocument, tags);
    enrichWithContainers(pointDocument, true);
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

    var pointDocument = new PointDocument();
    if (feature.canBePolygon()) {
      pointDocument.poiAreaNormalized = normalizeArea(feature.areaMeters());
    }
    pointDocument.poiSource = "OSM";
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

    setIconColorCategory(pointDocument, feature);

    if (pointDocument.poiIcon == "icon-search") {
      return false;
    }

    if (feature.getString("place") != null && pointDocument.poiCategory == "Wikipedia" && !feature.isPoint()) {
      return true;
    }

    convertTagsToDocument(pointDocument, feature);
    enrichWithContainers(pointDocument, false);
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

  private float normalizeArea(double areaM) {
    if (Double.isNaN(areaM) || areaM <= 0) {
      return 0f;
    }
    double norm = Math.log1p(areaM) / Math.log1p(1e11);
    return (float) Math.max(0.0, Math.min(1.0, norm));
  }

  private void addNonIconFeaturesToElasricseach(SourceFeature feature) throws GeometryException {
    if (!OsmNames.hasSearchableName(feature, this.context.supportedLanguages())) {
      return;
    }
    var category = OsmFeatureClassifier.classifyNonIcon(feature);
    if (category == null) {
      return;
    }
    var pointDocument = new PointDocument();
    pointDocument.poiIcon = category.icon;
    pointDocument.poiIconColor = category.color;
    pointDocument.poiCategory = category.poiCategory;
    pointDocument.poiSource = "OSM";
    var docId = sourceFeatureToDocumentId(feature);
    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };
    convertTagsToDocument(pointDocument, feature);
    enrichWithContainers(pointDocument, false);
    insertPointToElasticsearch(pointDocument, docId);
  }

  private void insertPointToElasticsearch(PointDocument pointDocument, String docId) {
    this.context.bulkListener().add(BulkOperation.of(op -> op
        .index(idx -> idx
            .index(this.context.pointsIndexTarget())
            .id(docId)
            .document(pointDocument))));
  }

  /**
   * Tags the point with the places it falls inside: the union of their names
   * (for "point, place" search), plus the tightest enclosing place and the
   * country (for display). Uses the container index loaded from the previous
   * build, so a first-ever build tags nothing and simply produces the index.
   */
  private void enrichWithContainers(PointDocument pointDocument, boolean isPlace) {
    if (pointDocument.location == null) {
      return;
    }
    var matches = this.context.containerIndex().containing(pointDocument.location[1], pointDocument.location[0]);
    if (matches.isEmpty()) {
      return;
    }
    ContainerRecord country = null;
    ContainerRecord container = null;
    Map<String, Set<String>> names = new LinkedHashMap<>();
    for (ContainerRecord match : matches) {
      match.names.forEach((lang, name) -> names.computeIfAbsent(lang, k -> new LinkedHashSet<>()).add(name));
      if (match.isCountry()) {
        if (country == null || match.area < country.area) {
          country = match;
        }
      } else if (!sharesNameForPlace(pointDocument, match, isPlace)
          && (container == null || match.area < container.area)) {
        container = match;
      }
    }
    Map<String, List<String>> parentNames = new LinkedHashMap<>();
    names.forEach((lang, set) -> parentNames.put(lang, new ArrayList<>(set)));
    pointDocument.poiParentNames = parentNames;
    if (country != null) {
      pointDocument.poiCountry = country.names;
    }
    if (container != null) {
      pointDocument.poiContainer = container.names;
    }
  }

  /**
   * Whether the container carries the same name as the point in any shared
   * language.
   * A place node commonly sits in a polygon of the same name; using it as
   * the container would display "X, X", so skip it and let a wider place win.
   */
  private static boolean sharesNameForPlace(PointDocument pointDocument, ContainerRecord container, boolean isPlace) {
    if (!isPlace) {
      return false;
    }
    for (var entry : pointDocument.name.entrySet()) {
      if (entry.getValue().equals(container.names.get(entry.getKey()))) {
        return true;
      }
    }
    return false;
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
      var lngLatCenterPoint = GeoUtils.worldToLatLonCoords(feature.centroid()).getCoordinate();
      bbox.center = new double[] { lngLatCenterPoint.getX(), lngLatCenterPoint.getY() };
      bbox.setBBox(simplified);
      for (String lang : supportedLanguages) {
        CoalesceIntoMap(bbox.name, lang, feature.getString("name:" + lang));
      }
      if (feature.hasTag("name")) {
        CoalesceIntoMap(bbox.name, "default", feature.getString("name"));
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
   * to read and write.
   */
  private static Geometry simplifyContainer(Geometry polygon) {
    try {
      return TopologyPreservingSimplifier.simplify(polygon, CONTAINER_SIMPLIFY_DEGREES);
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

  private void setIconColorCategory(PointDocument pointDocument, WithTags feature) {
    var category = OsmFeatureClassifier.classify(feature);
    pointDocument.poiIcon = category.icon;
    pointDocument.poiIconColor = category.color;
    pointDocument.poiCategory = category.poiCategory;
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
