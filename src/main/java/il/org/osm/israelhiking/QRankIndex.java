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

class QRankIndex {
  private static final Logger LOGGER = Logger.getLogger(QRankIndex.class.getName());

  private final LongIntHashMap qrankByQid;

  private QRankIndex(LongIntHashMap qrankByQid) {
    this.qrankByQid = qrankByQid;
  }

  // Empty index: every lookup returns 0. Lets local builds run without the 363 MB QRank file.
  static QRankIndex empty() {
    return new QRankIndex(new LongIntHashMap(0));
  }

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
      String line = br.readLine();
      while ((line = br.readLine()) != null) {
        int comma = line.indexOf(',');
        if (comma <= 1 || line.charAt(0) != 'Q') {
          continue;
        }
        try {
          long qid = Long.parseLong(line, 1, comma, 10);
          int qrank = Integer.parseInt(line, comma + 1, line.length(), 10);
          map.put(qid, qrank);
          rows++;
        } catch (NumberFormatException ignored) {
          // never fail the build over one stray line
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
