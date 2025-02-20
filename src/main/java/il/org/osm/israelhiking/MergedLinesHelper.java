package il.org.osm.israelhiking;

import com.onthegomap.planetiler.reader.SourceFeature;

class MergedLinesHelper {
    LineMerger lineMerger;
    SourceFeature feature;
    MergedLinesHelper() {
      this.lineMerger = new LineMerger();
    }
}