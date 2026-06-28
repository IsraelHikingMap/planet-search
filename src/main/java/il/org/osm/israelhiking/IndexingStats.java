package il.org.osm.israelhiking;

import java.util.concurrent.atomic.LongAdder;

final class IndexingStats {

  final LongAdder emittedCount = new LongAdder();
  final LongAdder indexedCount = new LongAdder();
  final LongAdder failedPointsCount = new LongAdder();
  final LongAdder failedBboxCount = new LongAdder();

  long getEmittedCount() {
    return emittedCount.sum();
  }

  long getIndexedCount() {
    return indexedCount.sum();
  }

  long getFailedCount() {
    return failedPointsCount.sum() + failedBboxCount.sum();
  }

  long getFailedPointsCount() {
    return failedPointsCount.sum();
  }

  long getFailedBboxCount() {
    return failedBboxCount.sum();
  }

  boolean hasIndexingFailures() {
    return failedPointsCount.sum() > 0;
  }
}
