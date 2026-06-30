package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("unit")
public class QRankLookupTest {

    private static Path writeGzip(Path dir, String content) throws IOException {
        Path gz = dir.resolve("qrank.csv.gz");
        try (OutputStream os = Files.newOutputStream(gz);
                GZIPOutputStream gzos = new GZIPOutputStream(os)) {
            gzos.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return gz;
    }

    @Test
    public void emptyLookupReturnsZero() {
        var lookup = QRankLookup.empty();
        assertEquals(0, lookup.qrankFor("Q665321"));
        assertEquals(0, lookup.size());
    }

    @Test
    public void nullPathYieldsEmptyLookupNoException() {
        var lookup = QRankLookup.load(null);
        assertEquals(0, lookup.size());
        assertEquals(0, lookup.qrankFor("Q42"));
    }

    @Test
    public void missingFileFailsFast(@TempDir Path dir) {
        var missing = dir.resolve("does-not-exist.csv.gz");
        assertThrows(IllegalArgumentException.class, () -> QRankLookup.load(missing),
                "a configured-but-missing qrank-path must fail fast, not silently floor every feature");
    }

    @Test
    public void looksUpVariousWikidataFormsFromOneFile(@TempDir Path dir) throws IOException {
        var lookup = QRankLookup.load(
                writeGzip(dir, "Entity,QRank\nQ665321,359540\nQ121211166,34\nQ42,1000000\n"));

        assertEquals(3, lookup.size());
        assertEquals(359540, lookup.qrankFor("Q665321"));
        assertEquals(34, lookup.qrankFor("Q121211166"));
        assertEquals(1000000, lookup.qrankFor("Q42"));
        assertEquals(1000000, lookup.qrankFor("Q42;Q43"),
                "a semicolon-joined wikidata tag must resolve to the first QID's rank");
        assertEquals(0, lookup.qrankFor("Q999"), "unknown id -> 0");
        assertEquals(0, lookup.qrankFor(null), "null -> 0");
        assertEquals(0, lookup.qrankFor(""), "empty -> 0");
        assertEquals(0, lookup.qrankFor("notaqid"), "malformed -> 0");
        assertEquals(0, lookup.qrankFor("Qx"), "a Q-prefixed but non-numeric id must return 0, not throw");
    }

    @Test
    public void headerOnlyFileFailsFast(@TempDir Path dir) throws IOException {
        Path gz = writeGzip(dir, "Entity,QRank\n");
        assertThrows(IllegalArgumentException.class, () -> QRankLookup.load(gz),
                "a configured file with no data rows must fail fast, not silently floor every feature");
    }

    @Test
    public void malformedDataRowsAreSkipped(@TempDir Path dir) throws IOException {
        var lookup = QRankLookup.load(writeGzip(dir, "Entity,QRank\nQ,5\nX9,5\nQ7,42\n"));
        assertEquals(1, lookup.size(), "a no-QID row and a non-Q row must both be skipped");
        assertEquals(42, lookup.qrankFor("Q7"));
        assertEquals(0, lookup.qrankFor("Q9"), "the X9 row must never have been indexed under Q9");
    }

    @Test
    public void rowWithNonNumericValueIsSkipped(@TempDir Path dir) throws IOException {
        var lookup = QRankLookup.load(writeGzip(dir, "Entity,QRank\nQ5,abc\nQ7,42\n"));
        assertEquals(1, lookup.size(), "a Q-row with an unparseable rank must be skipped, not abort the load");
        assertEquals(0, lookup.qrankFor("Q5"));
        assertEquals(42, lookup.qrankFor("Q7"), "the valid row after a bad one must still load");
    }

    @Test
    public void corruptFileFailsFast(@TempDir Path dir) throws IOException {
        Path notGzip = dir.resolve("qrank.csv.gz");
        Files.writeString(notGzip, "this is plainly not gzip data");
        assertThrows(IllegalArgumentException.class, () -> QRankLookup.load(notGzip),
                "a configured-but-corrupt file must fail fast, not silently floor every feature");
    }

    @Test
    public void truncatedGzipFailsFast(@TempDir Path dir) throws IOException {
        Path gz = writeGzip(dir, "Entity,QRank\nQ1,10\nQ2,20\nQ3,30\nQ4,40\n");
        byte[] full = Files.readAllBytes(gz);
        Files.write(gz, Arrays.copyOf(full, full.length - 6));
        assertThrows(IllegalArgumentException.class, () -> QRankLookup.load(gz),
                "a truncated/partial qrank.csv.gz must fail fast, not load a partial ranking");
    }
}
