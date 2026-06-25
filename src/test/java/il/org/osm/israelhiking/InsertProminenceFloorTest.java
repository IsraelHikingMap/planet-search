package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.WithTags;

import il.org.osm.israelhiking.ElasticsearchHelper.ElasticRunContext;

/** Pins that a document with no computed prominence is floored before it is indexed. */
@Tag("unit")
public class InsertProminenceFloorTest {

    private static final String[] LANGS = { "en", "he", "ru", "ar", "es" };

    private static PlanetSearchProfile profile() {
        var context = new ElasticRunContext(null, "points", "bbox", "points1", "bbox1", LANGS);
        return new PlanetSearchProfile(PlanetilerConfig.defaults(), context, QRankIndex.empty());
    }

    private static void invoke(String method, Class<?>[] sig, Object... args) throws Exception {
        Method m = PlanetSearchProfile.class.getDeclaredMethod(method, sig);
        m.setAccessible(true);
        m.invoke(profile(), args);
    }

    @Test
    public void nullProminenceIsFlooredToFloor() {
        float result = PlanetSearchProfile.flooredProminence(null);
        assertEquals((float) ProminenceCalculator.FLOOR, result, 1e-9f,
                "a null prominence must be floored to ProminenceCalculator.FLOOR (0.05)");
        assertEquals(0.05f, result, 1e-9f, "FLOOR is the documented 0.05 safety floor");
    }

    @Test
    public void flooredProminenceIsNeverNull() {

        Float result = PlanetSearchProfile.flooredProminence(null);
        assertNotNull(result, "floored prominence must never be null");
        assertFalse(Float.isNaN(result), "floored prominence must never be NaN");
    }

    @Test
    public void existingProminenceIsLeftUnchanged() {

        assertEquals(0.73f, PlanetSearchProfile.flooredProminence(0.73f), 1e-9f,
                "a real prominence must pass through unchanged");
    }

    @Test
    public void lowButNonNullProminenceIsLeftUnchanged() {

        assertEquals(0.01f, PlanetSearchProfile.flooredProminence(0.01f), 1e-9f,
                "the insert net only fills nulls; it must not re-floor an existing value");
    }

    @Test
    public void setProminenceReadsImageWebsiteAndWikidataSignals() throws Exception {
        WithTags feature = WithTags.from(Map.of("ele", "2000"));
        var doc = new PointDocument();
        doc.poiFeatureClass = "peak";
        doc.image = "img.jpg";
        doc.website = "https://example.org";
        doc.wikidata = "Q42";
        invoke("setProminence", new Class<?>[] { PointDocument.class, WithTags.class }, doc, feature);
        assertNotNull(doc.poiProminence);
        assertTrue(doc.poiProminence > (float) ProminenceCalculator.FLOOR,
                "a peak with image, website and wikidata must score above the bare floor");
    }

    @Test
    public void setProminenceTreatsWikimediaCommonsAsImageWhenImageAbsent() throws Exception {
        WithTags feature = WithTags.from(Map.of());
        var withCommons = new PointDocument();
        withCommons.poiFeatureClass = "peak";
        withCommons.wikimedia_commons = "File:Peak.jpg";
        invoke("setProminence", new Class<?>[] { PointDocument.class, WithTags.class }, withCommons, feature);

        var bare = new PointDocument();
        bare.poiFeatureClass = "peak";
        invoke("setProminence", new Class<?>[] { PointDocument.class, WithTags.class }, bare, feature);

        assertTrue(withCommons.poiProminence > bare.poiProminence,
                "wikimedia_commons alone must count as image richness");
    }

    @Test
    public void insertFloorsNullProminenceAndSwallowsTheIndexError() throws Exception {
        var doc = new PointDocument();
        invoke("insertPointToElasticsearch", new Class<?>[] { PointDocument.class, String.class },
                doc, "OSM_node_1");
        assertEquals((float) ProminenceCalculator.FLOOR, doc.poiProminence, 1e-9f,
                "the insert path floors a null prominence before indexing");
    }

    @Test
    public void floorLogicLivesInTheIndexerAtBuildTime() throws Exception {
        var m = PlanetSearchProfile.class.getDeclaredMethod("flooredProminence", Float.class);
        assertTrue(Modifier.isStatic(m.getModifiers()),
                "flooredProminence is a build-time static in the Java indexer module");
        assertEquals(float.class, m.getReturnType());

        assertEquals(0.05, ProminenceCalculator.FLOOR, 1e-9,
                "the indexer owns the documented 0.05 prominence floor");
    }
}
