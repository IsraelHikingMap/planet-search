package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Pins ProminenceCalculator.compute: class priors, QRank/elevation/metadata signals, floor and clamp. */
@Tag("unit")
public class ProminenceCalculatorTest {

    private static final double NO_ELE = Double.NaN;

    @Test
    public void famousPeakWithQRankRanksHigh() {

        float prominence = ProminenceCalculator.compute("peak",
                4302,  true,  false,  true,  359_540L);
        assertTrue(prominence > 0.7, "famous high peak should score high, was " + prominence);
    }

    @Test
    public void duplicateNodeWithNoSignalScoresLow() {

        float prominence = ProminenceCalculator.compute(null, NO_ELE, false, false, false, 0L);
        assertEquals(0.1625f, prominence, 1e-4, "a node with no signal should score at the baseline");

        float famous = ProminenceCalculator.compute("peak", 4302, true, false, true, 359_540L);
        assertTrue(famous > prominence * 3, "famous peak must dominate an unsignalled node");
    }

    @Test
    public void prominenceIsNeverZero() {

        float prominence = ProminenceCalculator.compute(null, NO_ELE, false, false, false, 0L);
        assertTrue(prominence >= ProminenceCalculator.FLOOR);
        assertTrue(prominence > 0f);
    }

    @Test
    public void cityOutranksVillage() {
        float city = ProminenceCalculator.compute("city", NO_ELE, false, false, false, 0L);
        float village = ProminenceCalculator.compute("village", NO_ELE, false, false, false, 0L);
        assertTrue(city > village, "city " + city + " should outrank village " + village);
    }

    @Test
    public void higherPeakOutranksLowerPeak() {
        float high = ProminenceCalculator.compute("peak", 4000, false, false, false, 0L);
        float low = ProminenceCalculator.compute("peak", 200, false, false, false, 0L);
        assertTrue(high > low, "a 4000m peak should outrank a 200m hill");
    }

    @Test
    public void parkScoresAboveGenericNode() {

        float park = ProminenceCalculator.compute("park", NO_ELE, false, false, false, 0L);
        float generic = ProminenceCalculator.compute(null, NO_ELE, false, false, false, 0L);
        assertTrue(park > generic);
    }

    @Test
    public void qrankOnlyCountsWithRawAboveOne() {

        float withQ = ProminenceCalculator.compute("spring", NO_ELE, false, false, true, 50_000L);
        float noQ = ProminenceCalculator.compute("spring", NO_ELE, false, false, true, 0L);
        float oneQ = ProminenceCalculator.compute("spring", NO_ELE, false, false, true, 1L);
        assertTrue(withQ > noQ, "a popular spring should beat an obscure one");
        assertEquals(noQ, oneQ, 1e-6, "a raw QRank of 1 contributes nothing, same as 0");
    }

    @Test
    public void negativeElevationScoresLikeSeaLevel() {

        float below = ProminenceCalculator.compute("peak", -413, false, false, false, 0L);
        float atSea = ProminenceCalculator.compute("peak", 0, false, false, false, 0L);
        assertEquals(atSea, below, 1e-6, "a below-sea-level peak should score like a sea-level peak");
        assertTrue(below > 0f && below <= 1f, "prominence must stay finite and in range, was " + below);
    }

    @Test
    public void outputAlwaysInUnitRange() {

        float prominence = ProminenceCalculator.compute("city", 8849, true, true, true, Long.MAX_VALUE);
        assertTrue(prominence >= 0f && prominence <= 1f, "prominence out of range: " + prominence);
    }

    private static float bare(String featureClass) {
        return ProminenceCalculator.compute(featureClass, NO_ELE, false, false, false, 0L);
    }

    @Test
    public void settlementPriorsAreOrdered() {

        assertTrue(bare("town") > bare("village"), "town must outrank village");
        assertTrue(bare("village") > bare("hamlet"), "village must outrank hamlet");
        assertTrue(bare("hamlet") > bare(null), "hamlet must outrank an unclassified node");

        assertEquals(0.05f + 0.45f * 0.80f, bare("town"), 1e-5f);
        assertEquals(0.05f + 0.45f * 0.35f, bare("hamlet"), 1e-5f);
    }

    @Test
    public void midTierPlaceClassesShareThe045Prior() {

        float expected = 0.05f + 0.45f * 0.45f;
        for (String fc : new String[] {"suburb", "neighbourhood", "island", "locality", "place"}) {
            assertEquals(expected, bare(fc), 1e-5f, fc + " should use the 0.45 prior");
        }
    }

    @Test
    public void poiAndHistoricClassesShareThe055Prior() {

        float expected = 0.05f + 0.45f * 0.55f;
        for (String fc : new String[] {"viewpoint", "attraction", "lodging", "historic"}) {
            assertEquals(expected, bare(fc), 1e-5f, fc + " should use the 0.55 prior");
        }
    }

    @Test
    public void clamp01HandlesNanNegativeAndOverflow() {

        assertEquals(0.0, ProminenceCalculator.clamp01(Double.NaN), 0.0, "NaN must clamp to 0");
        assertEquals(0.0, ProminenceCalculator.clamp01(-0.5), 0.0, "negative must clamp to 0");
        assertEquals(1.0, ProminenceCalculator.clamp01(1.7), 0.0, "above 1 must clamp to 1");
        assertEquals(0.42, ProminenceCalculator.clamp01(0.42), 1e-9, "in-range value passes through");
    }

    @Test
    public void unknownClassUsesTheDefaultPrior() {

        assertEquals(bare(null), bare("zzz-unknown-class"), 1e-6f,
                "an unknown class should score at the generic 0.25 prior, like null");
        assertEquals(0.05f + 0.45f * 0.25f, bare("zzz-unknown-class"), 1e-5f);
    }
}
