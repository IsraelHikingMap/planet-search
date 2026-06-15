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
