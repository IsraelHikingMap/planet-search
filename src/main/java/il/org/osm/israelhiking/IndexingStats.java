package il.org.osm.israelhiking;

import java.util.concurrent.atomic.LongAdder;

/**
 * Holds the bulk-indexing counters, kept out of PlanetSearchProfile (whose focus is OSM-feature ->
 * document conversion). emitted is incremented on each add(); indexed/failed are recorded by
 * AccountingBulkListener from the bulk callbacks. Reported as one summary line at the end of a run.
 */
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
