package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Guards the INSERT-path prominence safety floor (FR-1.4, AC1).
 *
 * <p>This is distinct from {@code ProminenceCalculatorTest.prominenceIsNeverZero}, which guards the
 * CALCULATOR floor. Some emit paths (ski-lift ways, relation-completion) never run
 * {@code convertTagsToDocument}, so {@code pointDocument.prominence} arrives null at
 * {@code insertPointToElasticsearch}. The insert-path floor must turn that null into
 * {@code (float) ProminenceCalculator.FLOOR} (0.05) so it can't serialize as null —
 * which, given {@code @JsonInclude(NON_NULL)}, would be OMITTED from _source -> field_value_factor
 * {@code missing:1.0} -> the doc unfairly outranks a real, scored feature (prominence &lt; 1).
 *
 * <p>The floored logic was extracted to the package-private static {@code flooredProminence(Float)}
 * (a behavior-preserving refactor — no contract/API change) so it can be unit-tested without
 * touching the private {@code insertPointToElasticsearch} / its real {@code BulkIngester}. No mock
 * is needed: the helper is pure.
 */
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
        // The whole point of the net: the result is a primitive float, never null, so the field
        // can't be omitted from _source under @JsonInclude(NON_NULL).
        Float result = PlanetSearchProfile.flooredProminence(null);
        assertNotNull(result, "floored prominence must never be null");
        assertFalse(Float.isNaN(result), "floored prominence must never be NaN");
    }

    @Test
    public void existingProminenceIsLeftUnchanged() {
        // The floor must NOT overwrite a real, computed value (it only fills the null gap).
        assertEquals(0.73f, PlanetSearchProfile.flooredProminence(0.73f), 1e-9f,
                "a real prominence must pass through unchanged");
    }

    @Test
    public void lowButNonNullProminenceIsLeftUnchanged() {
        // Even a value below the floor is a *real* computed value (ProminenceCalculator already
        // clamps to >= FLOOR); the insert-path net only fills nulls, it does not re-floor.
        assertEquals(0.01f, PlanetSearchProfile.flooredProminence(0.01f), 1e-9f,
                "the insert net only fills nulls; it must not re-floor an existing value");
    }

    /**
     * Build-time placement invariant (AC4, documentation-grade). These ranking-floor computations
     * live in the Java indexer ({@code vendor/planet-search}) and are exercised at index/emit time —
     * NOT at C# query time. Asserting the symbols exist here (package-private static on
     * {@code PlanetSearchProfile}, alongside {@code ProminenceCalculator.FLOOR}) pins that the
     * guarded logic is in the indexer module, reinforcing "never compute ranking signals at query
     * time."
     */
    @Test
    public void floorLogicLivesInTheIndexerAtBuildTime() throws Exception {
        var m = PlanetSearchProfile.class.getDeclaredMethod("flooredProminence", Float.class);
        assertTrue(Modifier.isStatic(m.getModifiers()),
                "flooredProminence is a build-time static in the Java indexer module");
        assertEquals(float.class, m.getReturnType());
        // FLOOR is the indexer-owned constant the insert path floors to.
        assertEquals(0.05, ProminenceCalculator.FLOOR, 1e-9,
                "the indexer owns the documented 0.05 prominence floor");
    }
}
