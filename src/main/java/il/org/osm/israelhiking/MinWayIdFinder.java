package il.org.osm.israelhiking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.linemerge.LineMerger;

import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;

class MergedFeature {
  Long minId;
  SourceFeature representingFeature;
  Geometry geometry;
  double length;
}

class MinWayIdFinder {
  List<Long> ids = new CopyOnWriteArrayList<Long>();
  List<SourceFeature> features = new CopyOnWriteArrayList<SourceFeature>();

  public List<MergedFeature> getMergedFeatures() throws GeometryException {
    if (features.isEmpty()) {
      return List.of();
    }

    // 1. Bulk merge - single LineMerger pass over all input geometries
    var lineMerger = new LineMerger();
    for (SourceFeature feature : features) {
      lineMerger.add(feature.worldGeometry());
    }
    @SuppressWarnings("unchecked")
    Collection<Geometry> mergedGeoms = lineMerger.getMergedLineStrings();

    // 2. Build coordinate -> merged geometry lookup by indexing ALL coordinates
    // of every merged output. This guarantees that any input segment's
    // endpoint will find its parent geometry regardless of whether the output
    // is an open line or a closed ring (where LineMerger may pick an
    // arbitrary start/end point).
    var coordToMerged = new HashMap<CoordKey, Geometry>();
    for (Geometry geom : mergedGeoms) {
      for (Coordinate coord : geom.getCoordinates()) {
        coordToMerged.put(key(coord), geom);
      }
    }

    // 3. Attribute each input feature to its output geometry via endpoint lookup,
    // aggregating minId, representingFeature, and length per output geometry.
    var mergedFeatureMap = new HashMap<Geometry, MergedFeature>(mergedGeoms.size() * 2);
    for (SourceFeature feature : features) {
      Coordinate[] coords = feature.worldGeometry().getCoordinates();
      if (coords.length == 0)
        continue;

      Geometry mergedGeom = coordToMerged.get(key(coords[0]));
      if (mergedGeom == null) {
        mergedGeom = coordToMerged.get(key(coords[coords.length - 1]));
      }
      if (mergedGeom == null)
        continue; // shouldn't happen, but be safe

      MergedFeature mf = mergedFeatureMap.computeIfAbsent(mergedGeom, g -> {
        var newMf = new MergedFeature();
        newMf.geometry = g;
        newMf.minId = Long.MAX_VALUE;
        newMf.length = 0;
        return newMf;
      });

      mf.length += feature.lengthMeters();
      if (feature.id() < mf.minId) {
        mf.minId = feature.id();
        mf.representingFeature = feature;
      }
    }

    features = new CopyOnWriteArrayList<>(); // release memory
    return new ArrayList<>(mergedFeatureMap.values());
  }

  private static CoordKey key(Coordinate c) {
    return new CoordKey(c);
  }

  private record CoordKey(long x, long y) {
    private static final double SCALE = 1e8; // ~0.01 mm precision

    CoordKey(Coordinate c) {
      this(Math.round(c.x * SCALE), Math.round(c.y * SCALE));
    }
  }
}