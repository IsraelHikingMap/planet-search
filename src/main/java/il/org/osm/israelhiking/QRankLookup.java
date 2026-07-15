package il.org.osm.israelhiking;

import com.carrotsearch.hppc.LongIntHashMap;
import com.onthegomap.planetiler.collection.Hppc;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QRankLookup {
  private static final Logger LOGGER = LoggerFactory.getLogger(QRankLookup.class);

  private final LongIntHashMap qrankByQid;

  private QRankLookup(LongIntHashMap qrankByQid) {
    this.qrankByQid = qrankByQid;
  }

  static QRankLookup empty() {
    return new QRankLookup(Hppc.newLongIntHashMap());
  }

  static QRankLookup load(Path csvGz) {
    if (csvGz == null) {
      LOGGER.info("QRankLookup: no QRank file configured — running with an empty lookup (lookups return 0)");
      return empty();
    }
    long startMs = System.currentTimeMillis();
    LongIntHashMap map = Hppc.newLongIntHashMap();
    long rows = 0;
    CsvParserSettings settings = new CsvParserSettings();
    settings.setHeaderExtractionEnabled(true);
    settings.setLineSeparatorDetectionEnabled(true);
    settings.setReadInputOnSeparateThread(false);
    settings.setAutoClosingEnabled(false);
    CsvParser parser = new CsvParser(settings);
    try (InputStream fis = Files.newInputStream(csvGz);
        GZIPInputStream gz = new GZIPInputStream(fis, 1 << 16);
        Reader reader = new InputStreamReader(gz, StandardCharsets.UTF_8)) {
      parser.beginParsing(reader);
      String[] row;
      while ((row = parser.parseNext()) != null) {
        if (row.length < 2 || row[0] == null || row[1] == null
            || row[0].isEmpty() || row[0].charAt(0) != 'Q') {
          continue;
        }
        try {
          long qid = Long.parseLong(row[0], 1, row[0].length(), 10);
          int qrank = Integer.parseInt(row[1]);
          map.put(qid, qrank);
          rows++;
        } catch (NumberFormatException ignored) {
        }
      }
      gz.transferTo(OutputStream.nullOutputStream());
    } catch (Exception e) {
      throw new IllegalArgumentException("QRankLookup: failed to read " + csvGz + " after " + rows
          + " rows; fix --qrank-path or omit it", e);
    }
    long heapMb = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024);
    LOGGER.info("QRankLookup: loaded {} rows from {} in {}ms (heap used ~{} MB)",
        rows, csvGz, System.currentTimeMillis() - startMs, heapMb);
    if (rows == 0) {
      throw new IllegalArgumentException("QRankLookup: " + csvGz
          + " yielded 0 usable rows (expected a gzipped 'Entity,QRank' CSV); fix --qrank-path or omit it");
    }
    return new QRankLookup(map);
  }

  long qrankFor(String wikidata) {
    if (wikidata == null || wikidata.length() < 2 || wikidata.charAt(0) != 'Q') {
      return 0;
    }
    int end = wikidata.indexOf(';');
    if (end < 0) {
      end = wikidata.length();
    }
    try {
      long qid = Long.parseLong(wikidata, 1, end, 10);
      return qrankByQid.getOrDefault(qid, 0);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  int size() {
    return qrankByQid.size();
  }
}
