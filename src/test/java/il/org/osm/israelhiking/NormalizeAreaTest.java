package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Pins normalizeArea's log-scaled [0,1] mapping and its degenerate-input guards. */
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
