package il.org.osm.israelhiking;

import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.RELATION;
import static com.onthegomap.planetiler.reader.osm.OsmElement.Type.WAY;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.math.NumberUtils;
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
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;

public class PlanetSearchProfile implements Profile {
  private static final Logger LOGGER = Logger.getLogger(PlanetSearchProfile.class.getName());

  private PlanetilerConfig config;
  private ElasticsearchClient esClient;
  private final String pointsIndexName;
  private final String bboxIndexName;
  private final String[] supportedLanguages;
  // QRank lookup (Wikimedia pageviews by wikidata id) for the prominence signal. Empty index when
  // no QRank file is provided (local builds), so lookups return 0.
  private final QRankIndex qrankIndex;

  // Bulk indexer: thread-safe, buffers operations and flushes them in batches. Planetiler emits
  // features from multiple worker threads, so BulkIngester (concurrent add() internally) fits
  // better than hand-locked BulkRequests.
  private final BulkIngester<Void> bulkIngester;
  // Indexing accounting counters + failure thresholds, kept in their own holder (not the focus of
  // this class). PlanetSearchProfile increments the emitted buckets on each emit; the listener
  // shares the same LongAdders to record indexed/failed/transient outcomes from the bulk callbacks.
  private final IndexingStats stats = new IndexingStats();

  public static final String POINTS_LAYER_NAME = "global_points";

  private static final Map<String, MinWayIdFinder> Singles = new ConcurrentHashMap<>();
  private static final Map<String, MinWayIdFinder> NamedHighways = new ConcurrentHashMap<>();
  private static final Map<String, MinWayIdFinder> Waterways = new ConcurrentHashMap<>();

  public PlanetSearchProfile(PlanetilerConfig config, ElasticsearchClient esClient, String pointsIndexName,
      String bboxIndexName, String[] supportedLnaguages, QRankIndex qrankIndex) {
    this.config = config;
    this.esClient = esClient;
    this.pointsIndexName = pointsIndexName;
    this.supportedLanguages = supportedLnaguages;
    this.bboxIndexName = bboxIndexName;
    this.qrankIndex = qrankIndex;
    this.bulkIngester = BulkIngester.of(b -> b
        .client(esClient)
        // Flush whenever any of these thresholds is hit.
        .maxOperations(5_000)
        .maxSize(5 * 1024 * 1024)
        .maxConcurrentRequests(4)
        // Listener gives per-batch visibility instead of swallowing errors. Extracted into a named
        // class (AccountingBulkListener) so the counting logic is unit-testable. Pass the whole
        // IndexingStats holder so the counters are wired by name, not a swap-prone positional list.
        .listener(new AccountingBulkListener(esClient, stats)));
  }

  /*
   * Two-pass processing: pass 1 stores node locations and (via preprocessOsmRelation) the relation
   * /mtb:name way info the profile needs later; pass 2 emits a point per relation and mtb:name way,
   * merging the ways and using the first point of the merged linestring.
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
   * Populate pointDocument.alt_names from the OSM variant-name tags, keyed by language like name:
   * suffixed tags (alt_name:lang, ...) under lang, bare tags under "default".
   *
   * Unlike CoalesceIntoMap, alt-name tags are frequently multi-valued with ";" (alt_name=A;B;C), so
   * we split on ";", trim, drop empties and de-dup (order-preserving) into a List so each variant is
   * a separate token. Built lazily, so a feature with no variant tags leaves alt_names null.
   */
  private void addAltNames(PointDocument pointDocument, WithTags feature) {
    var altNames = OsmTagUtils.buildAltNames(supportedLanguages, feature::getString);
    if (altNames != null) {
      pointDocument.alt_names = altNames;
    }
  }

  private void convertTagsToDocument(PointDocument pointDocument, WithTags feature) {
    convertTagsToDocument(pointDocument, feature, new OsmTagUtils.OsmTagCache(feature::getString));
  }

  // Cache-accepting overload: paths that also call setIconColorCategory on the same feature build one
  // OsmTagCache and share it here (used for feature_class) and there, so the tag reads happen once.
  private void convertTagsToDocument(PointDocument pointDocument, WithTags feature,
      OsmTagUtils.OsmTagCache tags) {
    for (String language : supportedLanguages) {
      CoalesceIntoMap(pointDocument.name, language, feature.getString("name:" + language));
      CoalesceIntoMap(pointDocument.description, language, feature.getString("description:" + language));
    }
    if (feature.hasTag("name")) {
      CoalesceIntoMap(pointDocument.name, "default", feature.getString("name"));
    }
    if (feature.hasTag("description")) {
      CoalesceIntoMap(pointDocument.description, "default", feature.getString("description"));
    }
    // Variant names into the separate alt_names field. convertTagsToDocument is the single
    // chokepoint that also serves relations, so this one call covers the relation path too.
    addAltNames(pointDocument, feature);
    setDifficulty(pointDocument, feature);
    pointDocument.wikidata = feature.getString("wikidata");
    pointDocument.image = feature.getString("image");
    pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
    pointDocument.website = feature.getString("website");
    setProminence(pointDocument, feature);
    setPopulation(pointDocument, feature);
    pointDocument.poiFeatureClass = OsmTagUtils.classifyFeatureClass(tags);
  }

  /**
   * Compute the composite prominence score from OSM tags + QRank and store it on the document. Reads
   * tags directly, not poiCategory, which is assigned later in some emit paths.
   */
  private void setProminence(PointDocument pointDocument, WithTags feature) {
    long qrankRaw = qrankIndex.getByWikidata(pointDocument.wikidata);

    // Elevation feeds the peak prominence prior, read from the OSM ele tag (NaN when absent).
    double ele = OsmTagUtils.parseFirstNumber(feature.getString("ele"));

    boolean hasImage = pointDocument.image != null || pointDocument.wikimedia_commons != null;
    boolean hasWebsite = pointDocument.website != null;
    boolean hasWikidata = pointDocument.wikidata != null;

    ProminenceCalculator.Result r = ProminenceCalculator.compute(
        feature.getString("natural"),
        feature.getString("place"),
        feature.getString("boundary"),
        feature.getString("tourism"),
        feature.getString("historic"),
        feature.getString("waterway"),
        ele, hasImage, hasWebsite, hasWikidata, qrankRaw);

    pointDocument.poiProminence = r.prominence;

    setEnrichmentSignals(pointDocument, feature);
  }

  /**
   * Additive ranking signals beyond the prominence composite:
   *   - area_norm: log-normalized polygon area, only for a polygon-capable SourceFeature; null for
   *     points/lines and for relations routed through convertTagsToDocument as WithTags.
   *   - intermittent: the OSM intermittent=yes flag (seasonal/dry water).
   */
  private void setEnrichmentSignals(PointDocument pointDocument, WithTags feature) {
    // area_norm — geometry-dependent, so only when we actually hold a polygon-capable SourceFeature.
    if (feature instanceof SourceFeature sf && sf.canBePolygon()) {
      try {
        pointDocument.poiAreaNorm = normalizeArea(sf.areaMeters());
      } catch (Exception e) {
        // Bad polygon geometry — leave area_norm null, never fail the build. Logged at FINE (not
        // WARNING) because a few unbuildable polygons is an expected data condition in this
        // high-volume path; FINE keeps it diagnosable without flooding the console.
        LOGGER.fine(() -> "area_norm skipped for " + sourceFeatureToDocumentId(sf)
            + " (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ")");
      }
    }

    // intermittent — only set the flag when present (NON_NULL keeps it off the other ~22.8M docs).
    if (feature.hasTag("intermittent", "yes")) {
      pointDocument.intermittent = Boolean.TRUE;
    }
  }

  /** log1p(areaM)/log1p(MAX_AREA_M2) clamped to [0,1]; MAX_AREA_M2 = 1e11 (~100k km²). */
  private static float normalizeArea(double areaM) {
    if (Double.isNaN(areaM) || areaM <= 0) {
      return 0f;
    }
    double norm = Math.log1p(areaM) / Math.log1p(1e11);
    return (float) Math.max(0.0, Math.min(1.0, norm));
  }

  /** Population is a place/admin-layer signal only — set it for settlements, leave POIs null. */
  private void setPopulation(PointDocument pointDocument, WithTags feature) {
    String place = feature.getString("place");
    if (place == null) {
      return;
    }
    double parsed = OsmTagUtils.parseFirstNumber(feature.getString("population"));
    if (!Double.isNaN(parsed) && parsed > 0) {
      pointDocument.population = (int) Math.min(parsed, Integer.MAX_VALUE);
      return;
    }
    // Ladder fallback when the population tag is missing (covers the ~80% of villages/hamlets
    // that have no number). A real value always overrides this.
    switch (place) {
      case "city":    pointDocument.population = 1_000_000; break;
      case "town":    pointDocument.population = 50_000; break;
      case "village": pointDocument.population = 2_000; break;
      case "hamlet":  pointDocument.population = 200; break;
      default:        pointDocument.population = 20; break;
    }
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
    var pointDocument = new PointDocument();
    var relationTags = new OsmTagUtils.OsmTagCache(relation::getString);
    setIconColorCategory(pointDocument, relation, relationTags);

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

    convertTagsToDocument(pointDocument, relation, relationTags);

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
      if (isBBoxFeature(feature, supportedLanguages)) {
        insertBboxToElasticsearch(feature, supportedLanguages);
      }
      processOsmRelationFeature(feature, features);
      if (processMtbNameFeature(feature, features))
        return;
      if (processWaterwayFeature(feature, features))
        return;
      if (processHighwayFeautre(feature, features))
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
    convertTagsToDocument(pointDocument, feature);
    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var docId = pointDocument.poiSource + "_" + feature.getString("identifier");
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

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
        convertTagsToDocument(pointDocument, minIdFeature);
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
        pointDocument.poiLength = mergedFeature.length;

        var firstLine = mergedFeature.geometry;
        var point = GeoUtils.point(((Geometry) firstLine).getCoordinate());
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

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
        convertTagsToDocument(pointDocument, minIdFeature);
        pointDocument.poiCategory = "Water";
        pointDocument.poiIcon = "icon-river";
        pointDocument.poiIconColor = "#1e80e3";
        pointDocument.poiSource = "OSM";
        pointDocument.poiLength = mergedFeature.length;

        var firstLine = mergedFeature.geometry;
        var point = GeoUtils.point(((Geometry) firstLine).getCoordinate());
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

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
        var minIdTags = new OsmTagUtils.OsmTagCache(minIdFeature::getString);
        setIconColorCategory(pointDocument, minIdFeature, minIdTags);
        convertTagsToDocument(pointDocument, minIdFeature, minIdTags);
        pointDocument.poiSource = "OSM";
        pointDocument.poiLength = mergedFeature.length;

        var point = GeoUtils.point((mergedFeature.geometry.getCoordinate()));
        var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
        pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };
        insertPointToElasticsearch(pointDocument, "OSM_way_" + mergedFeature.minId);
      }

      return true;
    }
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
    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());

    var pointDocument = new PointDocument();
    var featureTags = new OsmTagUtils.OsmTagCache(feature::getString);
    convertTagsToDocument(pointDocument, feature, featureTags);
    pointDocument.poiSource = "OSM";
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };

    setIconColorCategory(pointDocument, feature, featureTags);

    if (pointDocument.poiIcon == "icon-search") {
      return false;
    }

    if (feature.getString("place") != null && pointDocument.poiCategory == "Wikipedia" && !feature.isPoint()) {
      return true;
    }

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

    if (pointDocument.poiIcon == "icon-search"
        && ((feature.getString("wikidata") != null || feature.getString("wikipedia") != null))) {
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
    if (feature.hasTag("description")) {
      CoalesceIntoMap(pointDocument.description, "default", feature.getString("description"));
    }
    // Mirror convertTagsToDocument's variant-name population for the non-icon path, which builds
    // its own PointDocument inline.
    addAltNames(pointDocument, feature);
    pointDocument.wikidata = feature.getString("wikidata");
    pointDocument.image = feature.getString("image");
    pointDocument.wikimedia_commons = feature.getString("wikimedia_commons");
    pointDocument.website = feature.getString("website");
    pointDocument.poiSource = "OSM";
    var docId = sourceFeatureToDocumentId(feature);
    var point = feature.canBePolygon() ? (Point) feature.centroidIfConvex()
        : GeoUtils.point(feature.worldGeometry().getCoordinate());
    var lngLatPoint = GeoUtils.worldToLatLonCoords(point).getCoordinate();
    pointDocument.location = new double[] { lngLatPoint.getX(), lngLatPoint.getY() };
    // This inline path builds its own PointDocument and (unlike convertTagsToDocument) never ran the
    // ranking signals, so a wikidata-documented landmark caught here would otherwise go in with the
    // bare prominence FLOOR. Run prominence (which also sets area_norm/intermittent) + population +
    // feature_class here too, so every emit path carries the same ranking signals.
    setProminence(pointDocument, feature);
    setPopulation(pointDocument, feature);
    pointDocument.poiFeatureClass = OsmTagUtils.classifyFeatureClass(feature::getString);
    insertPointToElasticsearch(pointDocument, docId);
  }

  /**
   * Safety floor for the insert path: every emitted document must carry a prominence so the
   * query-time field_value_factor multiply is consistent. Some emit paths don't run
   * convertTagsToDocument, leaving prominence null -> missing:1.0 -> they'd unfairly beat a real,
   * scored feature. Floor a null to the minimum; leave any real value unchanged.
   */
  static float flooredProminence(Float prominence) {
    return prominence == null ? (float) ProminenceCalculator.FLOOR : prominence;
  }

  private void insertPointToElasticsearch(PointDocument pointDocument, String docId) {
    pointDocument.poiProminence = flooredProminence(pointDocument.poiProminence);
    stats.emittedCount.increment();
    bulkIngester.add(BulkOperation.of(op -> op
        .index(idx -> idx
            .index(this.pointsIndexName)
            .id(docId)
            .document(pointDocument))));
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
      var lngLatCenterPoint = GeoUtils.worldToLatLonCoords(feature.centroid()).getCoordinate();
      bbox.center = new double[] { lngLatCenterPoint.getX(), lngLatCenterPoint.getY() };
      bbox.setBBox(polygon);
      for (String lang : supportedLanguages) {
        CoalesceIntoMap(bbox.name, lang, feature.getString("name:" + lang));
      }
      if (feature.hasTag("name")) {
        CoalesceIntoMap(bbox.name, "default", feature.getString("name"));
      }
      String bboxDocId = sourceFeatureToDocumentId(feature);
      stats.emittedCount.increment();
      bulkIngester.add(BulkOperation.of(op -> op
          .index(idx -> idx
              .index(this.bboxIndexName)
              .id(bboxDocId)
              .document(bbox))));
    } catch (Exception e) {
      // Only geometry/serialization building can throw here; indexing errors are counted by the
      // bulk listener.
      LOGGER.warning(() -> "Failed to build bbox document for "
          + sourceFeatureToDocumentId(feature) + ": " + e.getMessage());
    }
  }

  /**
   * Flush every buffered document and wait for in-flight bulk requests to complete. Call after
   * planetiler.run() and before the alias swap / refresh, so the new index is fully populated when
   * it goes live. close() also flushes, but we flush explicitly first so the final flush is visible.
   */
  public void flush() {
    bulkIngester.flush();
    // close() blocks until the buffer and all in-flight requests (and their counter-updating
    // listener callbacks) have completed.
    bulkIngester.close();
  }

  // Counters delegate to IndexingStats so MainClass keeps a stable surface; reported in one summary
  // line after the run.

  public long getIndexedCount() {
    return stats.getIndexedCount();
  }

  public long getFailedCount() {
    return stats.getFailedCount();
  }

  public long getEmittedCount() {
    return stats.getEmittedCount();
  }

  /**
   * Get the first point of the trail relation via heuristics on its first member.
   *
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
   * Remove completed relation members from their parent super relations: when a new way completes a
   * relation, drop that relation from its parents so that eventually all ways/relations are empty
   * and can be emitted. Also tracks the first/second member features for finding the first point.
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
    for (String lang : supportedLanguages) {
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
        !feature.hasTag("place", "borough")) {
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

  // Convenience overload for callers that don't already hold a per-feature tag cache (solo icon
  // emits). Paths that also classify the same feature build ONE OsmTagCache and pass it to both this
  // method and classifyFeatureClass, so the ~21 tag reads happen once per feature, not per method.
  private void setIconColorCategory(PointDocument pointDocument, WithTags feature) {
    setIconColorCategory(pointDocument, feature, new OsmTagUtils.OsmTagCache(feature::getString));
  }

  private void setIconColorCategory(PointDocument pointDocument, WithTags feature,
      OsmTagUtils.OsmTagCache tags) {
    if ("protected_area".equals(tags.boundary) ||
        "national_park".equals(tags.boundary) ||
        "nature_reserve".equals(tags.leisure)) {
      setProtectedAreaIcon(pointDocument, feature);
      return;
    }
    if (tags.route != null) {
      switch (tags.route) {
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
        case "road":
          if ("yes".equals(tags.scenic)) {
            pointDocument.poiIconColor = "black";
            pointDocument.poiCategory = "4x4";
            pointDocument.poiIcon = "icon-four-by-four";
            return;
          }
      }
    }
    if (tags.historic != null) {
      pointDocument.poiIconColor = "#666666";
      pointDocument.poiCategory = "Historic";
      switch (tags.historic) {
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
    if ("picnic_table".equals(tags.leisure) ||
        "picnic_site".equals(tags.tourism) ||
        "picnic".equals(tags.amenity)) {
      pointDocument.poiIconColor = "#734a08";
      pointDocument.poiIcon = "icon-picnic";
      pointDocument.poiCategory = "Camping";
      return;
    }

    if (tags.natural != null) {
      switch (tags.natural) {
        case "cave_entrance":
          pointDocument.poiIconColor = "black";
          pointDocument.poiIcon = "icon-cave";
          pointDocument.poiCategory = "Natural";
          return;
        case "spring":
          pointDocument.poiIconColor = "#1e80e3";
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
          pointDocument.poiIconColor = "#1e80e3";
          pointDocument.poiIcon = "icon-waterhole";
          pointDocument.poiCategory = "Water";
          return;
      }
    }

    if ("reservoir".equals(tags.water) ||
        "pond".equals(tags.water) ||
        "lake".equals(tags.water) ||
        "stream_pool".equals(tags.water)) {
      pointDocument.poiIconColor = "#1e80e3";
      pointDocument.poiIcon = "icon-tint";
      pointDocument.poiCategory = "Water";
      return;
    }

    if (tags.manMade != null) {
      pointDocument.poiIconColor = "#1e80e3";
      pointDocument.poiCategory = "Water";
      switch (tags.manMade) {
        case "water_well":
          pointDocument.poiIcon = "icon-water-well";
          return;
        case "cistern":
          pointDocument.poiIcon = "icon-cistern";
          return;
      }
    }

    if ("waterfall".equals(tags.waterway)) {
      pointDocument.poiIconColor = "#1e80e3";
      pointDocument.poiIcon = "icon-waterfall";
      pointDocument.poiCategory = "Water";
      return;
    }

    if ("waterway".equals(tags.type)) {
      pointDocument.poiIconColor = "#1e80e3";
      pointDocument.poiIcon = "icon-river";
      pointDocument.poiCategory = "Water";
      return;
    }

    if (tags.place != null) {
      pointDocument.poiIconColor = "black";
      pointDocument.poiIcon = "icon-home";
      pointDocument.poiCategory = "Wikipedia";
      return;
    }

    if (tags.tourism != null) {
      switch (tags.tourism) {
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
        case "artwork":
          pointDocument.poiIconColor = "#ffb800";
          pointDocument.poiIcon = "icon-artwork";
          pointDocument.poiCategory = "Other";
          return;
        case "alpine_hut":
          pointDocument.poiIconColor = "#734a08";
          pointDocument.poiIcon = "icon-alpinehut";
          pointDocument.poiCategory = "Camping";
          return;
      }
    }

    // Treat natural=volcano like natural=peak so named summit nodes tagged as volcanoes get
    // icon-peak instead of the icon-search default (which would let processOtherSourceFeature drop
    // them). setFeatureClass and ProminenceCalculator already map volcano->peak.
    if ("peak".equals(tags.natural) || "volcano".equals(tags.natural)) {
      pointDocument.poiIconColor = "black";
      pointDocument.poiIcon = "icon-peak";
      pointDocument.poiCategory = "Natural";
      return;
    }

    if (tags.highway != null) {
      switch (tags.highway) {
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

    if ("place_of_worship".equals(tags.amenity) || "monastery".equals(tags.amenity)) {
      var religion = tags.religion != null ? tags.religion : "";
      pointDocument.poiCategory = "Other";
      pointDocument.poiIconColor = "black";
      switch (religion) {
        case "jewish":
          pointDocument.poiIcon = "icon-synagogue";
          return;
        case "christian":
          pointDocument.poiIcon = "icon-church";
          return;
        case "muslim":
          pointDocument.poiIcon = "icon-mosque";
          return;
        default:
          pointDocument.poiIcon = "icon-holy-place";
          return;
      }
    }

    if (tags.refILInature != null) {
      pointDocument.poiIconColor = "#116C00";
      pointDocument.poiIcon = "icon-inature";
      pointDocument.poiCategory = "iNature";
      return;
    }

    pointDocument.poiIconColor = "black";
    pointDocument.poiIcon = "icon-search";
    pointDocument.poiCategory = "Other";
  }

  /**
   * Differentiate protected areas by protection type so the dropdown no longer shows the same green
   * leaf for every park, monument, wilderness and forest. Keyed primarily off the free-text
   * protection_title (the most reliable discriminator on US data), with protect_class and boundary
   * as fallback; a plain park or nature reserve keeps the leaf. Uses only glyphs that exist in the
   * web font: icon-monument, icon-tree, icon-leaf.
   */
  private void setProtectedAreaIcon(PointDocument pointDocument, WithTags feature) {
    pointDocument.poiCategory = "Other";
    String title = feature.getString("protection_title");
    String t = title == null ? "" : title.toLowerCase(java.util.Locale.ROOT);
    String protectClass = feature.getString("protect_class");

    // National / Natural Monument — protect_class 3 is IUCN "Natural Monument".
    if (t.contains("monument") || "3".equals(protectClass)) {
      pointDocument.poiIconColor = "#734a08";
      pointDocument.poiIcon = "icon-monument";
      return;
    }
    // Wilderness (incl. "Wilderness Study Area") and forests — leafier green than a park boundary.
    if (t.contains("wilderness") || t.contains("forest") ||
        "forest".equals(feature.getString("boundary")) ||
        "national_forest".equals(feature.getString("boundary"))) {
      pointDocument.poiIconColor = "#006000";
      pointDocument.poiIcon = "icon-tree";
      return;
    }
    // Default: national/state park, nature reserve, protected landscape — keep the historical leaf.
    pointDocument.poiIconColor = "#008000";
    pointDocument.poiIcon = "icon-leaf";
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
