package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class InsertProminenceFloorTest {

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
    public void floorLogicLivesInTheIndexerAtBuildTime() throws Exception {
        var m = PlanetSearchProfile.class.getDeclaredMethod("flooredProminence", Float.class);
        assertTrue(Modifier.isStatic(m.getModifiers()),
                "flooredProminence is a build-time static in the Java indexer module");
        assertEquals(float.class, m.getReturnType());
        assertEquals(0.05, ProminenceCalculator.FLOOR, 1e-9,
                "the indexer owns the documented 0.05 prominence floor");
    }

    @Test
    public void unsetProminenceIsNonNullAfterFloor() throws Exception {
        PointDocument doc = new PointDocument();
        assertEquals(null, doc.poiProminence, "a doc that skipped setProminence starts null");
        doc.poiProminence = PlanetSearchProfile.flooredProminence(doc.poiProminence);
        assertNotNull(doc.poiProminence,
                "after the insert-path floor the field is non-null, so @JsonInclude(NON_NULL) keeps it");
        assertEquals((float) ProminenceCalculator.FLOOR, doc.poiProminence, 1e-9f);

        assertEquals(Float.class, PointDocument.class.getField("poiProminence").getType());
    }
}
