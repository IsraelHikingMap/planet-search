package il.org.osm.israelhiking;

import java.util.concurrent.atomic.LongAdder;

/** Indexing counters for a single Elasticsearch index. */
final class IndexingStats {

  final LongAdder emitted = new LongAdder();
  final LongAdder indexed = new LongAdder();
  final LongAdder failed = new LongAdder();

  long getEmitted() {
    return emitted.sum();
  }

  long getIndexed() {
    return indexed.sum();
  }

  long getFailed() {
    return failed.sum();
  }
}
