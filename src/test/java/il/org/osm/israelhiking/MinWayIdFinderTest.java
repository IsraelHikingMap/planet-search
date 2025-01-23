package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class MinWayIdFinderTest {

    @Test
    public void shouldFindMinWayWhenAddingBackwards() {
        var finder = new MinWayIdFinder();
        finder.addWayId(7);
        finder.addWayId(3);
        assertEquals(3, finder.minId);
    }

    @Test
    public void shouldFindMinWayWhenAddingForward() {
        var finder = new MinWayIdFinder();
        finder.addWayId(3);
        finder.addWayId(7);
        assertEquals(3, finder.minId);
    }
}
