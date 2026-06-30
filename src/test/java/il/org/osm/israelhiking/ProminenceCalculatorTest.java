package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import il.org.osm.israelhiking.OsmFeatureClassifier.Category;

@Tag("unit")
public class ProminenceCalculatorTest {

    private static final double NO_ELE = Double.NaN;

    @Test
    public void famousPeakWithQRankRanksHigh() {
        float prominence = ProminenceCalculator.compute(Category.PEAK, 4302, true, false, true, 359_540L);
        assertTrue(prominence > 0.7, "famous high peak should score high, was " + prominence);
    }

    @Test
    public void duplicateNodeWithNoSignalScoresLow() {
        float low = ProminenceCalculator.compute(Category.FALLBACK, NO_ELE, false, false, false, 0L);
        assertEquals(0.1625f, low, 1e-4, "a node with no signal should score at the baseline");
        float famous = ProminenceCalculator.compute(Category.PEAK, 4302, true, false, true, 359_540L);
        assertTrue(famous > low * 3, "famous peak must dominate an unsignalled node");
    }

    @Test
    public void prominenceIsNeverZero() {
        float prominence = ProminenceCalculator.compute(Category.FALLBACK, NO_ELE, false, false, false, 0L);
        assertTrue(prominence >= ProminenceCalculator.PROMINENCE_FLOOR);
        assertTrue(prominence > 0f);
    }

    @Test
    public void everyCategoryClearsTheFloor() {
        for (Category category : Category.values()) {
            float prominence = ProminenceCalculator.compute(category, NO_ELE, false, false, false, 0L);
            assertTrue(prominence >= (float) ProminenceCalculator.PROMINENCE_FLOOR,
                    category + " scored below the floor: " + prominence);
        }
    }

    @Test
    public void cityOutranksVillage() {
        float city = ProminenceCalculator.compute(Category.PLACE_CITY, NO_ELE, false, false, false, 0L);
        float village = ProminenceCalculator.compute(Category.PLACE_VILLAGE, NO_ELE, false, false, false, 0L);
        assertTrue(city > village, "city " + city + " should outrank village " + village);
    }

    @Test
    public void higherPeakOutranksLowerPeak() {
        float high = ProminenceCalculator.compute(Category.PEAK, 4000, false, false, false, 0L);
        float low = ProminenceCalculator.compute(Category.PEAK, 200, false, false, false, 0L);
        assertTrue(high > low, "a 4000m peak should outrank a 200m hill");
    }

    @Test
    public void nationalParkScoresAboveGenericNode() {
        float park = ProminenceCalculator.compute(Category.NATURE_RESERVE, NO_ELE, false, false, false, 0L);
        float generic = ProminenceCalculator.compute(Category.FALLBACK, NO_ELE, false, false, false, 0L);
        assertTrue(park > generic);
    }

    @Test
    public void qrankOnlyCountsWithWikidataPresence() {
        float withQ = ProminenceCalculator.compute(Category.NATURAL_SPRING, NO_ELE, false, false, true, 50_000L);
        float noQ = ProminenceCalculator.compute(Category.NATURAL_SPRING, NO_ELE, false, false, true, 0L);
        assertTrue(withQ > noQ, "a popular spring should beat an obscure one");
    }

    @Test
    public void qrankIgnoredWhenWikidataAbsent() {
        float withQrank = ProminenceCalculator.compute(Category.NATURAL_SPRING, NO_ELE, false, false, false, 1_000_000L);
        float withoutQrank = ProminenceCalculator.compute(Category.NATURAL_SPRING, NO_ELE, false, false, false, 0L);
        assertEquals(withoutQrank, withQrank, 1e-6f, "QRank must not affect a feature with no wikidata");
    }

    @Test
    public void belowSeaLevelPeakIsFlooredToZeroElevation() {
        float below = ProminenceCalculator.compute(Category.PEAK, -430, false, false, false, 0L);
        float atSeaLevel = ProminenceCalculator.compute(Category.PEAK, 0, false, false, false, 0L);
        assertEquals(atSeaLevel, below, 1e-6f,
                "a -430m peak must score the same as a 0m peak, not get a +430m elevation boost");
    }

    @Test
    public void peakScalesWithElevation() {
        float high = ProminenceCalculator.compute(Category.PEAK, 8849, false, false, false, 0L);
        float sea = ProminenceCalculator.compute(Category.PEAK, 0, false, false, false, 0L);
        assertTrue(high > sea, "elevation must lift a peak's prominence");
    }

    @Test
    public void outputAlwaysInUnitRange() {
        float prominence = ProminenceCalculator.compute(Category.PEAK, 8849, true, true, true, Long.MAX_VALUE);
        assertTrue(prominence >= 0f && prominence <= 1f, "prominence out of range: " + prominence);
    }

    @Test
    public void clamp01BoundsBothEnds() {
        assertEquals(0.0, ProminenceCalculator.clamp01(-1.0), 1e-9, "a negative input clamps to 0");
        assertEquals(1.0, ProminenceCalculator.clamp01(2.0), 1e-9, "an above-one input clamps to 1");
        assertEquals(0.5, ProminenceCalculator.clamp01(0.5), 1e-9, "an in-range value passes through");
    }
}
