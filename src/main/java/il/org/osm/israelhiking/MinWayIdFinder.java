package il.org.osm.israelhiking;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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
    var merged = false;
    var mergedFeatures = new ArrayList<MergedFeature>();
    for (SourceFeature feature : features) {
      var mergedFeature = new MergedFeature();
      mergedFeature.minId = feature.id();
      mergedFeature.representingFeature = feature;
      mergedFeature.geometry = feature.worldGeometry();
      mergedFeature.length = feature.lengthMeters();
      mergedFeatures.add(mergedFeature);
    }
    features = new CopyOnWriteArrayList<SourceFeature>(); // clear memory
    do {
      merged = false;
      for (int i = 0; i < mergedFeatures.size(); i++) {
        for (int j = i + 1; j < mergedFeatures.size(); j++) {
          var lineMerger = new LineMerger();
          var featureI = mergedFeatures.get(i);
          var featureJ = mergedFeatures.get(j);
          lineMerger.add(featureI.geometry);
          lineMerger.add(featureJ.geometry);
          if (lineMerger.getMergedLineStrings().size() == 1) {
            featureJ.geometry = (Geometry) lineMerger.getMergedLineStrings().iterator().next();
            featureJ.length += featureI.length;
            if (featureI.minId < featureJ.minId) {
              featureJ.minId = featureI.minId;
              featureJ.representingFeature = featureI.representingFeature;
            }
            mergedFeatures.remove(i);
            merged = true;
            break;
          }
        }
        if (merged)
          break;
      }
    } while (merged);
    return mergedFeatures;
  }
}