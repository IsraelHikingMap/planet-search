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
    public void rowWithoutCommaIsSkipped(@TempDir Path dir) throws IOException {
        // A line with no comma -> split(",",2) yields a single column -> cols.length != 2 -> skipped.
        // Pins the first sub-clause of the load-time guard, distinct from the length<2/non-Q clauses.
        Path gz = dir.resolve("qrank.csv.gz");
        try (OutputStream os = Files.newOutputStream(gz);
                GZIPOutputStream gzos = new GZIPOutputStream(os)) {
            gzos.write(("Entity,QRank\n"
                    + "Q665321\n"        // no comma -> one column -> cols.length != 2, skipped
                    + "Q42,1000000\n")
                    .getBytes(StandardCharsets.UTF_8));
        }
        var idx = QRankIndex.load(gz);
        assertEquals(1, idx.size(), "the comma-less row is skipped; only the valid row loads");
        assertEquals(0, idx.getByWikidata("Q665321"), "comma-less row never indexed");
        assertEquals(1000000, idx.getByWikidata("Q42"));
    }

    @Test
    public void longNonQPrefixedRowIsSkipped(@TempDir Path dir) throws IOException {
        // cols[0] of length >= 2 that does NOT start with 'Q' (e.g. "AB") must be skipped: this hits
        // the charAt(0) != 'Q' sub-clause specifically, which the length<2 rows short-circuit past.
        Path gz = dir.resolve("qrank.csv.gz");
        try (OutputStream os = Files.newOutputStream(gz);
                GZIPOutputStream gzos = new GZIPOutputStream(os)) {
            gzos.write(("Entity,QRank\n"
                    + "AB,999\n"          // length>=2 but not Q-prefixed -> charAt(0)!='Q', skipped
                    + "Q42,1000000\n")
                    .getBytes(StandardCharsets.UTF_8));
        }
        var idx = QRankIndex.load(gz);
        assertEquals(1, idx.size(), "the non-Q-prefixed row is skipped; only the valid row loads");
        assertEquals(1000000, idx.getByWikidata("Q42"));
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

    /** Build a gz index from "Q1,5\nQ42,1000000\n..." style lines (header prepended). */
    private static QRankIndex indexOf(Path dir, String rows) throws IOException {
        Path gz = dir.resolve("qrank.csv.gz");
        try (OutputStream os = Files.newOutputStream(gz);
                GZIPOutputStream gzos = new GZIPOutputStream(os)) {
            gzos.write(("Entity,QRank\n" + rows).getBytes(StandardCharsets.UTF_8));
        }
        return QRankIndex.load(gz);
    }

    @Test
    public void multiValueTakesMaxQRank(@TempDir Path dir) throws IOException {
        // Q42;Q64 -> max QRank wins, regardless of order.
        var idx = indexOf(dir, "Q42,1000000\nQ64,5\n");
        assertEquals(1000000, idx.getByWikidata("Q42;Q64"));
        assertEquals(1000000, idx.getByWikidata("Q64;Q42"));
    }

    @Test
    public void multiValueSkipsUnknownParts(@TempDir Path dir) throws IOException {
        var idx = indexOf(dir, "Q64,5\n");
        assertEquals(5, idx.getByWikidata("Q999;Q64"), "unknown part contributes 0, known part wins");
    }

    @Test
    public void whitespaceIsTrimmed(@TempDir Path dir) throws IOException {
        var idx = indexOf(dir, "Q42,1000000\nQ64,5\n");
        assertEquals(1000000, idx.getByWikidata(" Q42 "));
        assertEquals(1000000, idx.getByWikidata("Q42; Q64"), "space after the separator is trimmed");
    }

    @Test
    public void lowercaseQAccepted(@TempDir Path dir) throws IOException {
        var idx = indexOf(dir, "Q42,1000000\n");
        assertEquals(1000000, idx.getByWikidata("q42"));
    }

    @Test
    public void multiValueAllMalformedReturnsZero(@TempDir Path dir) throws IOException {
        var idx = indexOf(dir, "Q42,1000000\n");
        assertEquals(0, idx.getByWikidata("Q999;foo;"), "all parts unknown/malformed -> 0");
        assertEquals(0, idx.getByWikidata(";"));
        assertEquals(0, idx.getByWikidata("Q;Q"));
    }

    @Test
    public void wellPrefixedButNonNumericQidReturnsZero(@TempDir Path dir) throws IOException {
        // "Qabc" passes the length>=2 and leading-Q guard, then fails Long.parseLong inside qrankFor:
        // the lookup-side catch must swallow it and return 0, never throw. A valid sibling still wins.
        var idx = indexOf(dir, "Q42,1000000\n");
        assertEquals(0, idx.getByWikidata("Qabc"), "non-numeric Q-id must be caught and yield 0");
        assertEquals(0, idx.getByWikidata("Q12x"), "trailing non-digit must be caught and yield 0");
        assertEquals(1000000, idx.getByWikidata("Qabc;Q42"),
                "an unparseable part contributes 0; the valid part still wins");
    }

    @Test
    public void multiValueSeparatorEdgePositions(@TempDir Path dir) throws IOException {
        // The manual indexOf-walk differs from String.split() exactly at trailing/leading/double
        // separators (split drops trailing empties; the walk visits the empty tail). A valid Q-id
        // must still be found regardless of where empty parts sit around it.
        var idx = indexOf(dir, "Q42,1000000\nQ64,5\n");
        assertEquals(1000000, idx.getByWikidata("Q42;"), "trailing separator: valid part still found");
        assertEquals(1000000, idx.getByWikidata(";Q42"), "leading separator: empty first part skipped");
        assertEquals(1000000, idx.getByWikidata("Q42;;Q64"), "double separator: empty middle part skipped");
        assertEquals(1000000, idx.getByWikidata("Q42 ; Q64"), "spaces on both sides of the separator");
        assertEquals(1000000, idx.getByWikidata("Q999;Q42;"), "unknown + valid + empty trailing");
    }
}
