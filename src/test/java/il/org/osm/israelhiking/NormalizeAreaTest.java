package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Unit tests for PlanetSearchProfile.normalizeArea (the area -> [0,1] size proxy). */
@Tag("unit")
public class NormalizeAreaTest {

    @Test
    public void zeroAndNegativeAndNaNAreZero() {
        assertEquals(0f, PlanetSearchProfile.normalizeArea(0));
        assertEquals(0f, PlanetSearchProfile.normalizeArea(-100));
        assertEquals(0f, PlanetSearchProfile.normalizeArea(Double.NaN));
    }

    @Test
    public void outputIsClampedToUnitRange() {
        // A huge area beyond the 1e11 cap saturates at 1.0 rather than exceeding it.
        assertEquals(1f, PlanetSearchProfile.normalizeArea(1e15));
        float mid = PlanetSearchProfile.normalizeArea(1_000_000);
        assertTrue(mid > 0f && mid < 1f, "a finite area must land strictly inside (0,1)");
    }

    @Test
    public void largerAreaScoresHigher() {
        assertTrue(PlanetSearchProfile.normalizeArea(10_000_000)
                > PlanetSearchProfile.normalizeArea(10_000),
                "a bigger polygon must get a higher size proxy");
    }
}
