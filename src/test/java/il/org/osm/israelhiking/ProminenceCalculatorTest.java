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
        float prominence = ProminenceCalculator.compute("peak",
                4302, /*image*/ true, /*website*/ false, /*wikidata*/ true, /*qrank*/ 359_540L);
        assertTrue(prominence > 0.7, "famous high peak should score high, was " + prominence);
    }

    @Test
    public void duplicateNodeWithNoSignalScoresLow() {
        // The duplicate Pikes Peak node: same name, but no class, no ele, no wikidata/qrank.
        // It gets only the floor + the generic-node baseline (0.05 + 0.45*0.25 = 0.1625) — far below
        // a real classified/popular feature, which is what breaks the duplicate tie.
        float prominence = ProminenceCalculator.compute(null, NO_ELE, false, false, false, 0L);
        assertEquals(0.1625f, prominence, 1e-4, "a node with no signal should score at the baseline");

        float famous = ProminenceCalculator.compute("peak", 4302, true, false, true, 359_540L);
        assertTrue(famous > prominence * 3, "famous peak must dominate an unsignalled node");
    }

    @Test
    public void prominenceIsNeverZero() {
        // Floor guarantees a query-time multiply never annihilates a hit.
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
        // national_park/protected_area are classified as "park" by classifyFeatureClass.
        float park = ProminenceCalculator.compute("park", NO_ELE, false, false, false, 0L);
        float generic = ProminenceCalculator.compute(null, NO_ELE, false, false, false, 0L);
        assertTrue(park > generic);
    }

    @Test
    public void qrankOnlyCountsWithRawAboveOne() {
        // A raw QRank > 1 lifts the score; a 0/1 raw value contributes nothing — proven by the two
        // scores being equal when only the (<=1) qrank input differs.
        float withQ = ProminenceCalculator.compute("spring", NO_ELE, false, false, true, 50_000L);
        float noQ = ProminenceCalculator.compute("spring", NO_ELE, false, false, true, 0L);
        float oneQ = ProminenceCalculator.compute("spring", NO_ELE, false, false, true, 1L);
        assertTrue(withQ > noQ, "a popular spring should beat an obscure one");
        assertEquals(noQ, oneQ, 1e-6, "a raw QRank of 1 contributes nothing, same as 0");
    }

    @Test
    public void negativeElevationScoresLikeSeaLevel() {
        // Dead Sea-region peaks sit below sea level (ele < 0). The elevation is floored at 0 so log1p
        // never sees a negative argument (which would yield NaN). A below-sea-level peak then scores
        // identically to a sea-level peak — proving the clamp without reading an internal component.
        float below = ProminenceCalculator.compute("peak", -413, false, false, false, 0L);
        float atSea = ProminenceCalculator.compute("peak", 0, false, false, false, 0L);
        assertEquals(atSea, below, 1e-6, "a below-sea-level peak should score like a sea-level peak");
        assertTrue(below > 0f && below <= 1f, "prominence must stay finite and in range, was " + below);
    }

    @Test
    public void outputAlwaysInUnitRange() {
        // Even maxed-out inputs (top-prior class + max ele/qrank + all richness) stay clamped to [0,1].
        float prominence = ProminenceCalculator.compute("city", 8849, true, true, true, Long.MAX_VALUE);
        assertTrue(prominence >= 0f && prominence <= 1f, "prominence out of range: " + prominence);
    }

    /** Bare base-prior contribution for a class with no other signal: FLOOR + 0.45*prior. */
    private static float bare(String featureClass) {
        return ProminenceCalculator.compute(featureClass, NO_ELE, false, false, false, 0L);
    }

    @Test
    public void settlementPriorsAreOrdered() {
        // city(1.00) > town(0.80) > village(0.55) > hamlet(0.35) > generic(0.25).
        assertTrue(bare("town") > bare("village"), "town must outrank village");
        assertTrue(bare("village") > bare("hamlet"), "village must outrank hamlet");
        assertTrue(bare("hamlet") > bare(null), "hamlet must outrank an unclassified node");
        // exact floored values pin the town (0.80) and hamlet (0.35) arms.
        assertEquals(0.05f + 0.45f * 0.80f, bare("town"), 1e-5f);
        assertEquals(0.05f + 0.45f * 0.35f, bare("hamlet"), 1e-5f);
    }

    @Test
    public void midTierPlaceClassesShareThe045Prior() {
        // suburb/neighbourhood/island/locality/place all map to the 0.45 prior arm.
        float expected = 0.05f + 0.45f * 0.45f;
        for (String fc : new String[] {"suburb", "neighbourhood", "island", "locality", "place"}) {
            assertEquals(expected, bare(fc), 1e-5f, fc + " should use the 0.45 prior");
        }
    }

    @Test
    public void poiAndHistoricClassesShareThe055Prior() {
        // viewpoint/attraction/lodging/historic all map to the 0.55 prior arm.
        float expected = 0.05f + 0.45f * 0.55f;
        for (String fc : new String[] {"viewpoint", "attraction", "lodging", "historic"}) {
            assertEquals(expected, bare(fc), 1e-5f, fc + " should use the 0.55 prior");
        }
    }

    @Test
    public void clamp01HandlesNanNegativeAndOverflow() {
        // clamp01 is the last line of defence before the score reaches ES: NaN -> 0 (never poisons the
        // multiply), negatives -> 0, >1 -> 1, in-range passes through.
        assertEquals(0.0, ProminenceCalculator.clamp01(Double.NaN), 0.0, "NaN must clamp to 0");
        assertEquals(0.0, ProminenceCalculator.clamp01(-0.5), 0.0, "negative must clamp to 0");
        assertEquals(1.0, ProminenceCalculator.clamp01(1.7), 0.0, "above 1 must clamp to 1");
        assertEquals(0.42, ProminenceCalculator.clamp01(0.42), 1e-9, "in-range value passes through");
    }

    @Test
    public void unknownClassUsesTheDefaultPrior() {
        // An unrecognised feature_class falls to the 0.25 default arm — same as a null class.
        assertEquals(bare(null), bare("zzz-unknown-class"), 1e-6f,
                "an unknown class should score at the generic 0.25 prior, like null");
        assertEquals(0.05f + 0.45f * 0.25f, bare("zzz-unknown-class"), 1e-5f);
    }
}
