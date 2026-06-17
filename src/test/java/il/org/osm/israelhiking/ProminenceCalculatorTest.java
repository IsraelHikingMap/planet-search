package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class ProminenceCalculatorTest {

    private static final double NO_ELE = Double.NaN;

    @Test
    public void famousPeakWithQRankRanksHigh() {
        // Pikes Peak: a ~4300m peak with a strong QRank signal and an image.
        float prominence = ProminenceCalculator.compute("peak", null, null, null, null, null,
                4302, /*image*/ true, /*website*/ false, /*wikidata*/ true, /*qrank*/ 359_540L);
        assertTrue(prominence > 0.7, "famous high peak should score high, was " + prominence);
    }

    @Test
    public void duplicateNodeWithNoSignalScoresLow() {
        // The duplicate Pikes Peak node: same name, but no class, no ele, no wikidata/qrank.
        // It gets only the floor + the generic-node baseline (0.05 + 0.45*0.25 = 0.1625) — far below
        // a real classified/popular feature, which is what breaks the duplicate tie.
        float prominence = ProminenceCalculator.compute(null, null, null, null, null, null,
                NO_ELE, false, false, false, 0L);
        assertEquals(0.1625f, prominence, 1e-4, "a node with no signal should score at the baseline");

        float famous = ProminenceCalculator.compute("peak", null, null, null, null, null,
                4302, true, false, true, 359_540L);
        assertTrue(famous > prominence * 3, "famous peak must dominate an unsignalled node");
    }

    @Test
    public void prominenceIsNeverZero() {
        // Floor guarantees a query-time multiply never annihilates a hit.
        float prominence = ProminenceCalculator.compute(null, null, null, null, null, null,
                NO_ELE, false, false, false, 0L);
        assertTrue(prominence >= ProminenceCalculator.FLOOR);
        assertTrue(prominence > 0f);
    }

    @Test
    public void cityOutranksVillage() {
        float city = ProminenceCalculator.compute(null, "city", null, null, null, null,
                NO_ELE, false, false, false, 0L);
        float village = ProminenceCalculator.compute(null, "village", null, null, null, null,
                NO_ELE, false, false, false, 0L);
        assertTrue(city > village, "city " + city + " should outrank village " + village);
    }

    @Test
    public void higherPeakOutranksLowerPeak() {
        float high = ProminenceCalculator.compute("peak", null, null, null, null, null,
                4000, false, false, false, 0L);
        float low = ProminenceCalculator.compute("peak", null, null, null, null, null,
                200, false, false, false, 0L);
        assertTrue(high > low, "a 4000m peak should outrank a 200m hill");
    }

    @Test
    public void nationalParkScoresAboveGenericNode() {
        float park = ProminenceCalculator.compute(null, null, "national_park", null, null, null,
                NO_ELE, false, false, false, 0L);
        float generic = ProminenceCalculator.compute(null, null, null, null, null, null,
                NO_ELE, false, false, false, 0L);
        assertTrue(park > generic);
    }

    @Test
    public void qrankOnlyCountsWithRawAboveOne() {
        // A raw QRank > 1 lifts the score; a 0/1 raw value contributes nothing — proven by the two
        // scores being equal when only the (<=1) qrank input differs.
        float withQ = ProminenceCalculator.compute("spring", null, null, null, null, null,
                NO_ELE, false, false, true, 50_000L);
        float noQ = ProminenceCalculator.compute("spring", null, null, null, null, null,
                NO_ELE, false, false, true, 0L);
        float oneQ = ProminenceCalculator.compute("spring", null, null, null, null, null,
                NO_ELE, false, false, true, 1L);
        assertTrue(withQ > noQ, "a popular spring should beat an obscure one");
        assertEquals(noQ, oneQ, 1e-6, "a raw QRank of 1 contributes nothing, same as 0");
    }

    @Test
    public void negativeElevationScoresLikeSeaLevel() {
        // Dead Sea-region peaks sit below sea level (ele < 0). The elevation is floored at 0 so log1p
        // never sees a negative argument (which would yield NaN). A below-sea-level peak then scores
        // identically to a sea-level peak — proving the clamp without reading an internal component.
        float below = ProminenceCalculator.compute("peak", null, null, null, null, null,
                -413, false, false, false, 0L);
        float atSea = ProminenceCalculator.compute("peak", null, null, null, null, null,
                0, false, false, false, 0L);
        assertEquals(atSea, below, 1e-6, "a below-sea-level peak should score like a sea-level peak");
        assertTrue(below > 0f && below <= 1f, "prominence must stay finite and in range, was " + below);
    }

    @Test
    public void outputAlwaysInUnitRange() {
        // Even maxed-out inputs stay clamped to [0,1].
        float prominence = ProminenceCalculator.compute("peak", "city", "national_park", "viewpoint", "ruins",
                "stream", 8849, true, true, true, Long.MAX_VALUE);
        assertTrue(prominence >= 0f && prominence <= 1f, "prominence out of range: " + prominence);
    }
}
