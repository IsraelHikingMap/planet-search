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
    public void famousPeakWithWikipediaRanksHigh() {
        float prominence = ProminenceCalculator.compute(Category.PEAK, 4302, true, false, true, true);
        assertTrue(prominence > 0.7, "famous high peak should score high, was " + prominence);
    }

    @Test
    public void duplicateNodeWithNoSignalScoresLow() {
        float low = ProminenceCalculator.compute(Category.FALLBACK, NO_ELE, false, false, false, false);
        assertEquals(0.1625f, low, 1e-4, "a node with no signal should score at the baseline");
        float famous = ProminenceCalculator.compute(Category.PEAK, 4302, true, false, true, true);
        assertTrue(famous > low * 3, "famous peak must dominate an unsignalled node");
    }

    @Test
    public void prominenceIsNeverZero() {
        float prominence = ProminenceCalculator.compute(Category.FALLBACK, NO_ELE, false, false, false, false);
        assertTrue(prominence >= ProminenceCalculator.PROMINENCE_FLOOR);
        assertTrue(prominence > 0f);
    }

    @Test
    public void everyCategoryClearsTheFloor() {
        for (Category category : Category.values()) {
            float prominence = ProminenceCalculator.compute(category, NO_ELE, false, false, false, false);
            assertTrue(prominence >= (float) ProminenceCalculator.PROMINENCE_FLOOR,
                    category + " scored below the floor: " + prominence);
        }
    }

    @Test
    public void cityOutranksVillage() {
        float city = ProminenceCalculator.compute(Category.PLACE_CITY, NO_ELE, false, false, false, false);
        float village = ProminenceCalculator.compute(Category.PLACE_VILLAGE, NO_ELE, false, false, false, false);
        assertTrue(city > village, "city " + city + " should outrank village " + village);
    }

    @Test
    public void higherPeakOutranksLowerPeak() {
        float high = ProminenceCalculator.compute(Category.PEAK, 4000, false, false, false, false);
        float low = ProminenceCalculator.compute(Category.PEAK, 200, false, false, false, false);
        assertTrue(high > low, "a 4000m peak should outrank a 200m hill");
    }

    @Test
    public void nationalParkScoresAboveGenericNode() {
        float park = ProminenceCalculator.compute(Category.NATURE_RESERVE, NO_ELE, false, false, false, false);
        float generic = ProminenceCalculator.compute(Category.FALLBACK, NO_ELE, false, false, false, false);
        assertTrue(park > generic);
    }

    @Test
    public void wikipediaTagLiftsAFeature() {
        float withArticle = ProminenceCalculator.compute(Category.NATURAL_SPRING, NO_ELE, false, false, true, true);
        float withoutArticle = ProminenceCalculator.compute(Category.NATURAL_SPRING, NO_ELE, false, false, true, false);
        assertTrue(withArticle > withoutArticle, "a spring with a wikipedia article should beat one without");
    }

    @Test
    public void wikipediaTagOutweighsTheMetadata() {
        float famous = ProminenceCalculator.compute(Category.FALLBACK, NO_ELE, false, false, false, true);
        float tagged = ProminenceCalculator.compute(Category.FALLBACK, NO_ELE, true, true, true, false);
        assertTrue(famous > tagged,
                "an article should count for more than an image, a website and a wikidata id together");
    }

    @Test
    public void belowSeaLevelPeakIsFlooredToZeroElevation() {
        float below = ProminenceCalculator.compute(Category.PEAK, -430, false, false, false, false);
        float atSeaLevel = ProminenceCalculator.compute(Category.PEAK, 0, false, false, false, false);
        assertEquals(atSeaLevel, below, 1e-6f,
                "a -430m peak must score the same as a 0m peak, not get a +430m elevation boost");
    }

    @Test
    public void peakScalesWithElevation() {
        float high = ProminenceCalculator.compute(Category.PEAK, 8849, false, false, false, false);
        float sea = ProminenceCalculator.compute(Category.PEAK, 0, false, false, false, false);
        assertTrue(high > sea, "elevation must lift a peak's prominence");
    }

    @Test
    public void outputAlwaysInUnitRange() {
        float prominence = ProminenceCalculator.compute(Category.PEAK, 8849, true, true, true, true);
        assertTrue(prominence >= 0f && prominence <= 1f, "prominence out of range: " + prominence);
    }

    @Test
    public void clamp01BoundsBothEnds() {
        assertEquals(0.0, ProminenceCalculator.clamp01(-1.0), 1e-9, "a negative input clamps to 0");
        assertEquals(1.0, ProminenceCalculator.clamp01(2.0), 1e-9, "an above-one input clamps to 1");
        assertEquals(0.5, ProminenceCalculator.clamp01(0.5), 1e-9, "an in-range value passes through");
    }
}
