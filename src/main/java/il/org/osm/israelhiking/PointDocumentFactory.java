package il.org.osm.israelhiking;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.math.NumberUtils;

import com.onthegomap.planetiler.reader.WithTags;

/**
 * Turns the tags of an OSM element into a {@link PointDocument}: the names and
 * descriptions per language, the alternative names, the icon and category, the
 * difficulty and the prominence — everything a search document carries that
 * comes from the tags rather than from the geometry.
 *
 * This is its own class because the profile is no longer the only caller. A
 * street's document cannot be built while the input streams by, since the merge
 * does not know which of a street's many ways wins until the last one has gone
 * past; it is built at the end instead, from a second read of the input. Both
 * paths convert identically, from here.
 */
final class PointDocumentFactory {

  private final String[] supportedLanguages;
  private final QRankLookup qrankLookup;
  private final ContainerIndex containerIndex;

  PointDocumentFactory(String[] supportedLanguages, QRankLookup qrankLookup, ContainerIndex containerIndex) {
    this.supportedLanguages = supportedLanguages;
    this.qrankLookup = qrankLookup;
    this.containerIndex = containerIndex;
  }

  static final void CoalesceIntoMap(Map<String, String> map, String language, String... strings) {
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
    convertTagsToDocument(pointDocument, feature, OsmFeatureClassifier.classify(feature));
  }

  /**
   * The same conversion, for a caller that has already classified the feature
   * and wants that category to be the one the prominence is computed from —
   * without paying for a second classification and prominence on top of it.
   */
  private void convertTagsToDocument(PointDocument pointDocument, WithTags feature,
      OsmFeatureClassifier.Category category) {
    for (String language : this.supportedLanguages) {
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
    setProminence(pointDocument, feature, category);
    PlaceHelper.estimatePopulation(feature).ifPresent(population -> pointDocument.population = population);
  }

  private void setProminence(PointDocument pointDocument, WithTags feature,
      OsmFeatureClassifier.Category category) {
    long qrankRaw = this.qrankLookup.qrankFor(pointDocument.wikidata);
    double ele = OsmNumberParser.parseElevation(feature.getString("ele")).orElse(Double.NaN);
    boolean hasImage = pointDocument.image != null || pointDocument.wikimedia_commons != null;
    boolean hasWebsite = pointDocument.website != null;
    boolean hasWikidata = pointDocument.wikidata != null;

    pointDocument.poiProminence = ProminenceCalculator.compute(
        category, ele, hasImage, hasWebsite, hasWikidata, qrankRaw);
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

  private static void setIconColorCategory(PointDocument pointDocument, OsmFeatureClassifier.Category category) {
    pointDocument.poiIcon = category.icon;
    pointDocument.poiIconColor = category.color;
    pointDocument.poiCategory = category.poiCategory;
  }

  /**
   * How much of the world a polygon covers, as a 0..1 signal — a national park
   * outranks a bench of the same name.
   */
  private static float normalizeArea(double areaM) {
    if (Double.isNaN(areaM) || areaM <= 0) {
      return 0f;
    }
    double norm = Math.log1p(areaM) / Math.log1p(1e11);
    return (float) Math.max(0.0, Math.min(1.0, norm));
  }

  /**
   * Sets the area signal from a polygon's area in m², leaving it unset for a
   * feature that is not a polygon.
   *
   * @param areaMeters the area, or null when the feature is not a polygon
   */
  private static void setAreaNormalized(PointDocument pointDocument, Double areaMeters) {
    if (areaMeters != null) {
      pointDocument.poiAreaNormalized = normalizeArea(areaMeters);
    }
  }

  // ---------------------------------------------------------------------------
  // One method per kind of document the build indexes. Each returns a document
  // enriched with the places it falls in, ready to be indexed; emitting it to
  // Elasticsearch and to the tiles is the profile's business, not this class's.
  // ---------------------------------------------------------------------------

  /**
   * The document for a hiking, cycling, 4x4 or river route relation. Built
   * while relations are pre-processed, before any member way has been seen, so
   * it carries no location and is not enriched yet — the profile does both once
   * the relation's members have given it a geometry.
   *
   * @param category the relation's category, already classified by the caller
   *                 to decide this is a route worth keeping at all
   */
  PointDocument buildRouteRelationDocument(WithTags relation, OsmFeatureClassifier.Category category) {
    var pointDocument = new PointDocument();
    setIconColorCategory(pointDocument, category);
    convertTagsToDocument(pointDocument, relation, category);
    pointDocument.poiSource = "OSM";
    return pointDocument;
  }

  /**
   * The document for a feature from the external GeoJSON file, whose icon,
   * category and source are given by the file rather than derived from OSM
   * tags. The difficulty is set before the tag conversion on purpose: a feature
   * that also carries an OSM difficulty tag is described by the tag.
   */
  PointDocument buildExternalDocument(WithTags feature, double lng, double lat) {
    var pointDocument = new PointDocument();
    pointDocument.poiIcon = feature.getString("poiIcon");
    pointDocument.poiIconColor = feature.getString("poiIconColor");
    pointDocument.poiCategory = feature.getString("poiCategory");
    pointDocument.poiSource = feature.getString("poiSource");
    pointDocument.poiDifficulty = feature.getString("poiDifficulty");
    pointDocument.poiLength = NumberUtils.toDouble(feature.getString("poiLength"), 0.0);
    pointDocument.location = new double[] { lng, lat };
    convertTagsToDocument(pointDocument, feature);
    this.containerIndex.enrich(pointDocument, false);
    return pointDocument;
  }

  /**
   * The document for a single track, merged from every way sharing one
   * {@code mtb:name}. The mtb name wins over the plain name, which is often the
   * name of the path the track runs along.
   */
  PointDocument buildMtbDocument(WithTags way, double lng, double lat, double length) {
    var pointDocument = new PointDocument();
    pointDocument.poiCategory = "Bicycle";
    pointDocument.poiIcon = "icon-bike";
    pointDocument.poiIconColor = "gray";
    pointDocument.poiSource = "OSM";
    pointDocument.poiLength = length;
    pointDocument.location = new double[] { lng, lat };
    convertTagsToDocument(pointDocument, way);
    for (String language : this.supportedLanguages) {
      CoalesceIntoMap(pointDocument.name, language, way.getString("mtb:name:" + language));
    }
    if (way.hasTag("mtb:name")) {
      CoalesceIntoMap(pointDocument.name, "default", way.getString("mtb:name"));
    }
    this.containerIndex.enrich(pointDocument, false);
    return pointDocument;
  }

  /** The document for a river, merged from every way sharing its name. */
  PointDocument buildWaterwayDocument(WithTags way, double lng, double lat, double length) {
    var pointDocument = new PointDocument();
    pointDocument.poiCategory = "Water";
    pointDocument.poiIcon = "icon-river";
    pointDocument.poiIconColor = "#1e80e3";
    pointDocument.poiSource = "OSM";
    pointDocument.poiLength = length;
    pointDocument.location = new double[] { lng, lat };
    convertTagsToDocument(pointDocument, way);
    this.containerIndex.enrich(pointDocument, false);
    return pointDocument;
  }

  /**
   * The document for a named track, path, footway or cycleway, merged from
   * every way sharing its name.
   */
  PointDocument buildNamedHighwayDocument(WithTags way, double lng, double lat, double length) {
    var pointDocument = new PointDocument();
    var category = OsmFeatureClassifier.classify(way);
    setIconColorCategory(pointDocument, category);
    pointDocument.poiSource = "OSM";
    pointDocument.poiLength = length;
    pointDocument.location = new double[] { lng, lat };
    convertTagsToDocument(pointDocument, way, category);
    this.containerIndex.enrich(pointDocument, false);
    return pointDocument;
  }

  /**
   * The document for a settlement, park or other place. Enriched as a place, so
   * a place is not given itself as its own container.
   *
   * @param areaMeters the polygon's area, or null when the place is a node
   */
  PointDocument buildPlaceDocument(WithTags feature, double lng, double lat, Double areaMeters) {
    var pointDocument = new PointDocument();
    setAreaNormalized(pointDocument, areaMeters);
    pointDocument.poiSource = "OSM";
    pointDocument.location = new double[] { lng, lat };
    var category = OsmFeatureClassifier.classify(feature);
    setIconColorCategory(pointDocument, category);
    convertTagsToDocument(pointDocument, feature, category);
    this.containerIndex.enrich(pointDocument, true);
    return pointDocument;
  }

  /**
   * The document for an ordinary point of interest — the flow most OSM features
   * with an icon go through.
   *
   * @param category   the feature's category, already classified by the caller
   *                   to decide it has an icon worth indexing
   * @param areaMeters the polygon's area, or null when the feature is not a
   *                   polygon
   */
  PointDocument buildPointOfInterestDocument(WithTags feature, double lng, double lat,
      OsmFeatureClassifier.Category category, Double areaMeters) {
    var pointDocument = new PointDocument();
    setAreaNormalized(pointDocument, areaMeters);
    pointDocument.poiSource = "OSM";
    pointDocument.location = new double[] { lng, lat };
    setIconColorCategory(pointDocument, category);
    convertTagsToDocument(pointDocument, feature, category);
    this.containerIndex.enrich(pointDocument, false);
    return pointDocument;
  }

  /**
   * The document for a named feature that has no icon of its own — searchable,
   * but never drawn on the map.
   *
   * The prominence is deliberately computed from the feature's ordinary
   * classification and not from the icon-less category it is displayed under:
   * the icon-less category exists to decide how to show the feature, not how
   * notable it is.
   */
  PointDocument buildNonIconDocument(WithTags feature, double lng, double lat,
      OsmFeatureClassifier.Category category) {
    var pointDocument = new PointDocument();
    setIconColorCategory(pointDocument, category);
    pointDocument.poiSource = "OSM";
    pointDocument.location = new double[] { lng, lat };
    convertTagsToDocument(pointDocument, feature);
    this.containerIndex.enrich(pointDocument, false);
    return pointDocument;
  }

  /**
   * The document for one merged street, from the way that represents it and the
   * point recorded for that street while the input streamed by. Called once per
   * street from the street flush, off the input pass.
   *
   * Unlike {@link #buildNonIconDocument}, a street's prominence does come from
   * its icon-less category: streets are deliberately the least prominent thing
   * in the index, so a street never outranks a point of interest of the same
   * name.
   */
  PointDocument buildStreetDocument(WithTags way, double lng, double lat) {
    var pointDocument = new PointDocument();
    pointDocument.poiSource = "OSM";
    pointDocument.location = new double[] { lng, lat };
    var category = OsmFeatureClassifier.classifyNonIcon(way);
    setIconColorCategory(pointDocument, category);
    convertTagsToDocument(pointDocument, way, category);
    this.containerIndex.enrich(pointDocument, false);
    return pointDocument;
  }
}
