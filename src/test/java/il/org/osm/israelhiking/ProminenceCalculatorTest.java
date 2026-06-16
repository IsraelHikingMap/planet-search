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
        var r = ProminenceCalculator.compute("peak", null, null, null, null, null,
                4302, /*image*/ true, /*website*/ false, /*wikidata*/ true, /*qrank*/ 359_540L);
        assertTrue(r.prominence > 0.7, "famous high peak should score high, was " + r.prominence);
        assertTrue(r.eleNorm > 0.9, "4302m should normalize near the top, was " + r.eleNorm);
        assertTrue(r.qrankNorm > 0.8, "359k QRank should log-normalize high, was " + r.qrankNorm);
    }

    @Test
    public void duplicateNodeWithNoSignalScoresLow() {
        // The duplicate Pikes Peak node: same name, but no class, no ele, no wikidata/qrank.
        // It gets only the floor + the generic-node baseline (0.05 + 0.45*0.25 = 0.1625) — low,
        // and far below a real classified/popular feature, which is what breaks the duplicate tie.
        var r = ProminenceCalculator.compute(null, null, null, null, null, null,
                NO_ELE, false, false, false, 0L);
        assertEquals(0.1625f, r.prominence, 1e-4, "a node with no signal should score at the baseline");
        assertEquals(0f, r.qrankNorm, 1e-6);
        // Crucially, much lower than a famous peak (validated separately) so the multiply reorders it down.
        var famous = ProminenceCalculator.compute("peak", null, null, null, null, null,
                4302, true, false, true, 359_540L);
        assertTrue(famous.prominence > r.prominence * 3, "famous peak must dominate an unsignalled node");
    }

    @Test
    public void prominenceIsNeverZero() {
        // Floor guarantees a query-time multiply never annihilates a hit.
        var r = ProminenceCalculator.compute(null, null, null, null, null, null,
                NO_ELE, false, false, false, 0L);
        assertTrue(r.prominence >= ProminenceCalculator.FLOOR);
        assertTrue(r.prominence > 0f);
    }

    @Test
    public void cityOutranksVillage() {
        var city = ProminenceCalculator.compute(null, "city", null, null, null, null,
                NO_ELE, false, false, false, 0L);
        var village = ProminenceCalculator.compute(null, "village", null, null, null, null,
                NO_ELE, false, false, false, 0L);
        assertTrue(city.prominence > village.prominence,
                "city " + city.prominence + " should outrank village " + village.prominence);
    }

    @Test
    public void higherPeakOutranksLowerPeak() {
        var high = ProminenceCalculator.compute("peak", null, null, null, null, null,
                4000, false, false, false, 0L);
        var low = ProminenceCalculator.compute("peak", null, null, null, null, null,
                200, false, false, false, 0L);
        assertTrue(high.prominence > low.prominence,
                "a 4000m peak should outrank a 200m hill");
    }

    @Test
    public void nationalParkScoresAboveGenericNode() {
        var park = ProminenceCalculator.compute(null, null, "national_park", null, null, null,
                NO_ELE, false, false, false, 0L);
        var generic = ProminenceCalculator.compute(null, null, null, null, null, null,
                NO_ELE, false, false, false, 0L);
        assertTrue(park.prominence > generic.prominence);
    }

    @Test
    public void qrankOnlyCountsWithWikidataPresence() {
        // qrankRaw>1 contributes regardless, but a 0/1 raw value must not.
        var withQ = ProminenceCalculator.compute("spring", null, null, null, null, null,
                NO_ELE, false, false, true, 50_000L);
        var noQ = ProminenceCalculator.compute("spring", null, null, null, null, null,
                NO_ELE, false, false, true, 0L);
        assertTrue(withQ.prominence > noQ.prominence, "a popular spring should beat an obscure one");
        assertEquals(0f, noQ.qrankNorm, 1e-6);
    }

    @Test
    public void negativeElevationIsClampedToZeroNorm() {
        // Dead Sea-region peaks sit below sea level (ele < 0). Math.max(0, ele) must floor the
        // elevation at 0 so log1p never sees a negative argument (which would yield NaN). The peak
        // then gets its class prior with no elevation boost, identical to a sea-level peak.
        var below = ProminenceCalculator.compute("peak", null, null, null, null, null,
                -413, false, false, false, 0L);
        var atSea = ProminenceCalculator.compute("peak", null, null, null, null, null,
                0, false, false, false, 0L);
        assertEquals(0f, below.eleNorm, 1e-6, "negative elevation should normalize to 0, not NaN");
        assertEquals(atSea.prominence, below.prominence, 1e-6,
                "a below-sea-level peak should score like a sea-level peak");
        assertTrue(below.prominence > 0f && below.prominence <= 1f,
                "prominence must stay finite and in range, was " + below.prominence);
    }

    @Test
    public void outputAlwaysInUnitRange() {
        // Even maxed-out inputs stay clamped to [0,1].
        var r = ProminenceCalculator.compute("peak", "city", "national_park", "viewpoint", "ruins",
                "stream", 8849, true, true, true, Long.MAX_VALUE);
        assertTrue(r.prominence >= 0f && r.prominence <= 1f, "prominence out of range: " + r.prominence);
        assertTrue(r.base >= 0f && r.base <= 1f);
        assertTrue(r.qrankNorm >= 0f && r.qrankNorm <= 1f);
    }
}
