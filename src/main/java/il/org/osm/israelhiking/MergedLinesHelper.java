package il.org.osm.israelhiking;

import org.locationtech.jts.operation.linemerge.LineMerger;

import com.onthegomap.planetiler.reader.SourceFeature;

class MergedLinesHelper {
    LineMerger lineMerger;
    SourceFeature feature;
    MergedLinesHelper() {
      this.lineMerger = new LineMerger();
    }
}