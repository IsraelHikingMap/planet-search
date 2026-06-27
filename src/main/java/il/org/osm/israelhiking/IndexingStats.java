package il.org.osm.israelhiking;

import java.util.concurrent.atomic.LongAdder;

final class IndexingStats {

  final LongAdder indexedCount = new LongAdder();
  final LongAdder failedCount = new LongAdder();
  final LongAdder emittedCount = new LongAdder();

  long getIndexedCount() {
    return indexedCount.sum();
  }

  long getFailedCount() {
    return failedCount.sum();
  }

  long getEmittedCount() {
    return emittedCount.sum();
  }
}
