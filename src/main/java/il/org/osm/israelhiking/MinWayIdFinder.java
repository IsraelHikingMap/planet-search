package il.org.osm.israelhiking;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.locationtech.jts.operation.linemerge.LineMerger;

import com.onthegomap.planetiler.reader.SourceFeature;

class MinWayIdFinder {
  List<Long> ids = new CopyOnWriteArrayList<Long>();
  long minId;
  LineMerger lineMerger;
  double length;
  /**
   * The feature that reporesens the merged line. In our cause, the feature with
   * the minimal id.
   */
  SourceFeature representingFeature;

  public MinWayIdFinder() {
    this.minId = Integer.MAX_VALUE;
    this.lineMerger = new LineMerger();
    this.length = 0;
  }

  public void addWayId(long id) {
    ids.add(id);
    if (id < minId) {
      minId = id;
    }
  }
}