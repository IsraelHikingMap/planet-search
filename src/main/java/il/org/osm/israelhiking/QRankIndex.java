package il.org.osm.israelhiking;

import com.carrotsearch.hppc.LongIntHashMap;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/** QRank by Wikidata Q-id, from qrank.csv.gz (https://qrank.toolforge.org, CC0). */
class QRankIndex {
  private static final Logger LOGGER = Logger.getLogger(QRankIndex.class.getName());

  private final LongIntHashMap qrankByQid;

  private QRankIndex(LongIntHashMap qrankByQid) {
    this.qrankByQid = qrankByQid;
  }

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
    boolean partial = false;
    try (InputStream fis = Files.newInputStream(csvGz);
        GZIPInputStream gz = new GZIPInputStream(fis, 1 << 16);
        BufferedReader br = new BufferedReader(new InputStreamReader(gz, StandardCharsets.UTF_8), 1 << 20)) {
      String line = br.readLine();
      if (line != null && (line.length() < 2 || line.charAt(0) != 'Q')) {
        line = br.readLine();
      }
      for (; line != null; line = br.readLine()) {
        String[] cols = line.split(",", 2);
        if (cols.length != 2 || cols[0].length() < 2 || cols[0].charAt(0) != 'Q') {
          continue;
        }
        try {
          map.put(Long.parseLong(cols[0].substring(1)), Integer.parseInt(cols[1].trim()));
          rows++;
        } catch (NumberFormatException ignored) {
        }
      }
    } catch (Exception e) {
      partial = true;
      LOGGER.severe("QRankIndex: PARTIAL load of " + csvGz + " (" + e.getClass().getSimpleName()
          + ": " + e.getMessage() + ") — only " + rows
          + " rows loaded before failure; ranking will be skewed. Re-download the file.");
    }
    long heapMb = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024);
    if (!partial) {
      LOGGER.info("QRankIndex: loaded " + rows + " rows from " + csvGz
          + " in " + (System.currentTimeMillis() - startMs) + "ms (heap used ~" + heapMb + " MB)");
    }
    return new QRankIndex(map);
  }

  long getByWikidata(String wikidata) {
    if (wikidata == null) {
      return 0;
    }
    return Arrays.stream(wikidata.split(";"))
        .mapToLong(part -> qrankFor(part.trim()))
        .max()
        .orElse(0);
  }

  private long qrankFor(String token) {
    if (token.length() < 2 || (token.charAt(0) != 'Q' && token.charAt(0) != 'q')) {
      return 0;
    }
    try {
      return qrankByQid.getOrDefault(Long.parseLong(token.substring(1)), 0);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  int size() {
    return qrankByQid.size();
  }
}
