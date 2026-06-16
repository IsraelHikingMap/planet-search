package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("unit")
public class QRankIndexTest {

    @Test
    public void emptyIndexReturnsZero() {
        var idx = QRankIndex.empty();
        assertEquals(0, idx.getByWikidata("Q665321"));
        assertEquals(0, idx.size());
    }

    @Test
    public void nullPathYieldsEmptyIndexNoException() {
        var idx = QRankIndex.load(null);
        assertEquals(0, idx.size());
        assertEquals(0, idx.getByWikidata("Q42"));
    }

    @Test
    public void missingFileYieldsEmptyIndex(@TempDir Path dir) {
        var idx = QRankIndex.load(dir.resolve("does-not-exist.csv.gz"));
        assertEquals(0, idx.size());
    }

    @Test
    public void parsesGzippedCsvAndLooksUpByWikidata(@TempDir Path dir) throws IOException {
        Path gz = dir.resolve("qrank.csv.gz");
        try (OutputStream os = Files.newOutputStream(gz);
                GZIPOutputStream gzos = new GZIPOutputStream(os)) {
            gzos.write("Entity,QRank\nQ665321,359540\nQ121211166,34\nQ42,1000000\n"
                    .getBytes(StandardCharsets.UTF_8));
        }
        var idx = QRankIndex.load(gz);
        assertEquals(3, idx.size());
        assertEquals(359540, idx.getByWikidata("Q665321"));
        assertEquals(34, idx.getByWikidata("Q121211166"));
        assertEquals(1000000, idx.getByWikidata("Q42"));
    }

    @Test
    public void skipsMalformedRowsWithoutFailing(@TempDir Path dir) throws IOException {
        // Rows that fail the comma<=1 guard or the non-numeric parse must be skipped, never throw,
        // and never inflate the row count. Only the two well-formed rows survive.
        Path gz = dir.resolve("qrank.csv.gz");
        try (OutputStream os = Files.newOutputStream(gz);
                GZIPOutputStream gzos = new GZIPOutputStream(os)) {
            gzos.write(("Entity,QRank\n"
                    + "Q,100\n"        // comma at index 1 -> comma <= 1, skipped
                    + ",100\n"         // comma at index 0 -> comma <= 1, skipped
                    + "A,123\n"        // does not start with 'Q', skipped
                    + "Q42,abc\n"      // well-formed prefix but non-numeric rank -> NumberFormatException, skipped
                    + "Q665321,359540\n"
                    + "Q1,5\n")
                    .getBytes(StandardCharsets.UTF_8));
        }
        var idx = QRankIndex.load(gz);
        assertEquals(2, idx.size(), "only the two well-formed Q-rows should load");
        assertEquals(359540, idx.getByWikidata("Q665321"));
        assertEquals(5, idx.getByWikidata("Q1"));
        assertEquals(0, idx.getByWikidata("Q42"), "row with non-numeric rank was skipped");
    }

    @Test
    public void corruptGzipYieldsEmptyIndexNoException(@TempDir Path dir) throws IOException {
        // A file that is not valid gzip must not fail the build: load() catches, logs, and returns
        // whatever loaded (here nothing). The .gz extension lures the reader past the regular-file guard.
        Path gz = dir.resolve("qrank.csv.gz");
        Files.write(gz, "this is not gzip".getBytes(StandardCharsets.UTF_8));
        var idx = QRankIndex.load(gz);
        assertEquals(0, idx.size(), "an unreadable file should yield an empty index, not throw");
        assertEquals(0, idx.getByWikidata("Q42"));
    }

    @Test
    public void unknownOrMalformedWikidataReturnsZero(@TempDir Path dir) throws IOException {
        Path gz = dir.resolve("qrank.csv.gz");
        try (OutputStream os = Files.newOutputStream(gz);
                GZIPOutputStream gzos = new GZIPOutputStream(os)) {
            gzos.write("Entity,QRank\nQ1,5\n".getBytes(StandardCharsets.UTF_8));
        }
        var idx = QRankIndex.load(gz);
        assertEquals(0, idx.getByWikidata("Q999"), "unknown id -> 0");
        assertEquals(0, idx.getByWikidata(null), "null -> 0");
        assertEquals(0, idx.getByWikidata(""), "empty -> 0");
        assertEquals(0, idx.getByWikidata("notaqid"), "malformed -> 0");
        assertTrue(idx.getByWikidata("Q1") > 0);
    }
}
