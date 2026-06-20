package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class IsPlainTrailIconTest {

    @Test
    public void plainTrailIconsAreSkipped() {
        assertTrue(PlanetSearchProfile.isPlainTrailIcon("icon-hike"));
        assertTrue(PlanetSearchProfile.isPlainTrailIcon("icon-bike"));
        assertTrue(PlanetSearchProfile.isPlainTrailIcon("icon-four-by-four"));
    }

    @Test
    public void nonTrailIconsAreEmitted() {
        assertFalse(PlanetSearchProfile.isPlainTrailIcon("icon-peak"));
        assertFalse(PlanetSearchProfile.isPlainTrailIcon("icon-river"));
        assertFalse(PlanetSearchProfile.isPlainTrailIcon("icon-search"));
        assertFalse(PlanetSearchProfile.isPlainTrailIcon(""));
    }

    @Test
    public void nullIconIsEmitted() {
        assertFalse(PlanetSearchProfile.isPlainTrailIcon(null));
    }

    @Test
    public void valueEqualityNotReferenceEquality() {
        assertTrue(PlanetSearchProfile.isPlainTrailIcon(new String("icon-hike")));
    }
}
