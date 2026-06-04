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
        var r = ProminenceCalculator.compute("peak", null, null, null, null, null,
                4302, /*image*/ true, /*website*/ false, /*wikidata*/ true, /*qrank*/ 359_540L);
        assertTrue(r.prominence > 0.7, "famous high peak should score high, was " + r.prominence);
        assertTrue(r.eleNorm > 0.9, "4302m should normalize near the top, was " + r.eleNorm);
        assertTrue(r.qrankNorm > 0.8, "359k QRank should log-normalize high, was " + r.qrankNorm);
    }

    @Test
    public void duplicateNodeWithNoSignalScoresLow() {
        var r = ProminenceCalculator.compute(null, null, null, null, null, null,
                NO_ELE, false, false, false, 0L);
        assertEquals(0.1625f, r.prominence, 1e-4, "a node with no signal should score at the baseline");
        assertEquals(0f, r.qrankNorm, 1e-6);
        var famous = ProminenceCalculator.compute("peak", null, null, null, null, null,
                4302, true, false, true, 359_540L);
        assertTrue(famous.prominence > r.prominence * 3, "famous peak must dominate an unsignalled node");
    }

    @Test
    public void prominenceIsNeverZero() {
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
        var withQ = ProminenceCalculator.compute("spring", null, null, null, null, null,
                NO_ELE, false, false, true, 50_000L);
        var noQ = ProminenceCalculator.compute("spring", null, null, null, null, null,
                NO_ELE, false, false, true, 0L);
        assertTrue(withQ.prominence > noQ.prominence, "a popular spring should beat an obscure one");
        assertEquals(0f, noQ.qrankNorm, 1e-6);
    }

    @Test
    public void outputAlwaysInUnitRange() {
        var r = ProminenceCalculator.compute("peak", "city", "national_park", "viewpoint", "ruins",
                "stream", 8849, true, true, true, Long.MAX_VALUE);
        assertTrue(r.prominence >= 0f && r.prominence <= 1f, "prominence out of range: " + r.prominence);
        assertTrue(r.base >= 0f && r.base <= 1f);
        assertTrue(r.qrankNorm >= 0f && r.qrankNorm <= 1f);
    }
}
