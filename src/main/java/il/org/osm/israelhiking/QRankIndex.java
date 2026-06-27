package il.org.osm.israelhiking;

import com.carrotsearch.hppc.LongIntHashMap;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * In-memory lookup of QRank (Wikimedia pageview rank) by Wikidata Q-id.
 *
 * <p>QRank (https://qrank.toolforge.org, CC0) ranks Wikidata entities by aggregated Wikimedia
 * pageviews — a strong "how much do people look this up" signal, better aligned with an outdoor
 * product than Wikipedia-inlink importance. The file {@code qrank.csv.gz} has a header
 * {@code Entity,QRank} and ~28.7M rows like {@code Q665321,1234567}.
 *
 * <p>Backed by carrotsearch HPPC {@link LongIntHashMap} (a planetiler-bundled dependency — no new
 * pom entry). QRank's max (~559M) fits an {@code int}. ~28.7M entries ≈ 0.4–0.6 GB resident.
 *
 * <p>The {@link #load(Path)} factory accepts a null/missing/empty path and returns an EMPTY index
 * whose lookups all return 0 — the escape hatch for local testing on a Geofabrik extract without
 * shipping the 363 MB file. The map is built once before the planetiler pass and only read
 * (concurrently, safely) during it.
 */
class QRankIndex {
  private static final Logger LOGGER = Logger.getLogger(QRankIndex.class.getName());

  private final LongIntHashMap qrankByQid;

  private QRankIndex(LongIntHashMap qrankByQid) {
    this.qrankByQid = qrankByQid;
  }

  /** An empty index — every lookup returns 0. Used when no QRank file is provided. */
  static QRankIndex empty() {
    return new QRankIndex(new LongIntHashMap(0));
  }

  /**
   * Load a gzipped {@code qrank.csv.gz}. If {@code csvGz} is null, missing, or empty, returns an
   * empty index (no exception) so local builds can run without the file.
   */
  static QRankIndex load(Path csvGz) {
    if (csvGz == null || !Files.isRegularFile(csvGz)) {
      LOGGER.info("QRankIndex: no QRank file provided — running with an empty index (lookups return 0)");
      return empty();
    }
    long startMs = System.currentTimeMillis();
    LongIntHashMap map = new LongIntHashMap(32_000_000);
    long rows = 0;
    try (InputStream fis = Files.newInputStream(csvGz);
        GZIPInputStream gz = new GZIPInputStream(fis, 1 << 16);
        BufferedReader br = new BufferedReader(new InputStreamReader(gz, StandardCharsets.UTF_8), 1 << 20)) {
      String line = br.readLine(); // header: Entity,QRank
      while ((line = br.readLine()) != null) {
        int comma = line.indexOf(',');
        if (comma <= 1 || line.charAt(0) != 'Q') {
          continue; // skip malformed / non-Q rows
        }
        try {
          long qid = Long.parseLong(line, 1, comma, 10); // skip the leading 'Q', no substring alloc
          int qrank = Integer.parseInt(line, comma + 1, line.length(), 10);
          map.put(qid, qrank);
          rows++;
        } catch (NumberFormatException ignored) {
          // a stray line that doesn't parse — skip it, never fail the build
        }
      }
    } catch (Exception e) {
      LOGGER.warning("QRankIndex: failed to read " + csvGz + " (" + e.getMessage()
          + ") — continuing with whatever loaded (" + rows + " rows)");
    }
    long heapMb = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024);
    LOGGER.info("QRankIndex: loaded " + rows + " rows from " + csvGz
        + " in " + (System.currentTimeMillis() - startMs) + "ms (heap used ~" + heapMb + " MB)");
    return new QRankIndex(map);
  }

  /**
   * Raw QRank for an OSM {@code wikidata} tag value (e.g. {@code "Q665321"}), or 0 if the tag is
   * absent, malformed, or not in the table.
   */
  long getByWikidata(String wikidata) {
    if (wikidata == null || wikidata.length() < 2 || wikidata.charAt(0) != 'Q') {
      return 0;
    }
    try {
      long qid = Long.parseLong(wikidata, 1, wikidata.length(), 10);
      return qrankByQid.getOrDefault(qid, 0);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  int size() {
    return qrankByQid.size();
  }
}
