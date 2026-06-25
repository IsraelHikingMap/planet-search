package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Pins parseFirstNumber's defensive free-text number parsing. */
@Tag("unit")
public class ParseFirstNumberTest {

    private static double parse(String s) {
        return OsmTagUtils.parseFirstNumber(s);
    }

    @Test
    public void plainInteger() {
        assertEquals(4302.0, parse("4302"), 1e-9);
    }

    @Test
    public void decimal() {
        assertEquals(4302.3, parse("4302.3"), 1e-9);
    }

    @Test
    public void thousandsSeparatorComma() {
        assertEquals(14115.0, parse("14,115"), 1e-9);
    }

    @Test
    public void thousandsSeparatorSpace() {
        assertEquals(1000.0, parse("1 000"), 1e-9);
    }

    @Test
    public void numberWithUnitSuffix() {
        assertEquals(14115.0, parse("14115 ft"), 1e-9);
    }

    @Test
    public void yesIsNotANumber() {
        assertTrue(Double.isNaN(parse("yes")));
    }

    @Test
    public void nullIsNaN() {
        assertTrue(Double.isNaN(parse(null)));
    }

    @Test
    public void emptyIsNaN() {
        assertTrue(Double.isNaN(parse("")));
    }

    @Test
    public void rangeTakesFirstNumber() {

        assertEquals(100.0, parse("100-200"), 1e-9);
    }

    @Test
    public void secondDotEndsTheNumber() {

        assertEquals(1.2, parse("1.2.3"), 1e-9);
        assertEquals(192.168, parse("192.168.0.1"), 1e-9);
    }

    @Test
    public void pureNonNumericIsNaN() {
        assertTrue(Double.isNaN(parse("abc")));
    }

    @Test
    public void commaThousandsWithUnitSuffix() {

        assertEquals(1234.0, parse("1,234 m"), 1e-9);
    }

    @Test
    public void negativeElevation() {

        assertEquals(-413.0, parse("-413"), 1e-9);
    }

    @Test
    public void negativeWithUnitSuffix() {
        assertEquals(-413.0, parse("-413 m"), 1e-9);
    }

    @Test
    public void loneMinusIsNaN() {
        assertTrue(Double.isNaN(parse("-")));
    }

    @Test
    public void minusThenNonNumericIsNaN() {
        assertTrue(Double.isNaN(parse("-abc")));
    }

    @Test
    public void gluedMagnitudeSuffixMultiplies() {

        assertEquals(1_500_000.0, parse("1.5M"), 1e-9);
        assertEquals(50_000.0, parse("50k"), 1e-9);
        assertEquals(2_000_000.0, parse("2M"), 1e-9);
        assertEquals(3_200.0, parse("3.2k"), 1e-9);
        assertEquals(2_000_000_000.0, parse("2B"), 1e-9);
        assertEquals(10_000.0, parse("10K"), 1e-9);
    }

    @Test
    public void negativeWithMagnitudeSuffixMultiplies() {
        assertEquals(-2_000_000.0, parse("-2M"), 1e-9);
    }

    @Test
    public void multiplierEndsTheNumber_trailingCharsIgnored() {

        assertEquals(50_000.0, parse("50kk"), 1e-9);
        assertEquals(1_000_000.0, parse("1MM"), 1e-9);
        assertEquals(1_500.0, parse("1.5km"), 1e-9);
        assertEquals(1_500_000.0, parse("1.5M5"), 1e-9);
        assertEquals(50_000.0, parse("50K100"), 1e-9);
        assertEquals(1_000_000.0, parse("1.M"), 1e-9);
    }

    @Test
    public void multiplierBeforeDigitsIsIgnored() {

        assertEquals(50.0, parse("M50"), 1e-9);
        assertEquals(50.0, parse("k50"), 1e-9);
    }

    @Test
    public void onlyKkMbAreMultipliers_otherLettersAreTrailingUnits() {
        assertEquals(1.5, parse("1.5G"), 1e-9);
        assertEquals(1.0, parse("1b"), 1e-9);
        assertEquals(1.0, parse("1g"), 1e-9);
        assertEquals(1_000_000.0, parse("1Mb"), 1e-9);
    }

    @Test
    public void spaceSeparatedMagnitudeLetterDoesNotMultiply() {

        assertEquals(1.5, parse("1.5 M"), 1e-9);
        assertEquals(1.5, parse("1.5 km"), 1e-9);
        assertEquals(1.5, parse("1.5\tM"), 1e-9);
    }

    @Test
    public void gluedMetresUnitStillParses() {
        assertEquals(1500.0, parse("1500m"), 1e-9);
        assertEquals(4302.0, parse("4302m"), 1e-9);
        assertEquals(5.0, parse("5m"), 1e-9);
        assertEquals(100.0, parse("100m2"), 1e-9);
        assertEquals(5.0, parse("5mi"), 1e-9);
        assertEquals(1.5, parse("1.5mm"), 1e-9);
    }

    @Test
    public void unitSeparatedBySpaceStillParses() {
        assertEquals(14115.0, parse("14,115 ft"), 1e-9);
        assertEquals(1200.0, parse("1200 m"), 1e-9);
    }

    @ParameterizedTest(name = "\"{0}\" -> NaN")
    @ValueSource(strings = {
        " ", "  ", "--", "- ", "no", ".", "..", "-.", "k", "M", "kg",
        "١٢٣"
    })
    public void variousNonNumbersAreNaN(String input) {
        assertTrue(Double.isNaN(parse(input)), "expected NaN for \"" + input + "\"");
    }

    @Test
    public void leadingZerosAndDotQuirks() {
        assertEquals(0.0, parse("0"), 1e-9);
        assertEquals(0.0, parse("00"), 1e-9);
        assertEquals(7.0, parse("007"), 1e-9);
        assertEquals(5.0, parse("5."), 1e-9);
        assertEquals(5.0, parse(".5"), 1e-9);
        assertEquals(0.0, parse("-0"), 1e-9);
    }

    @Test
    public void separatorRunsAreSwallowed() {
        assertEquals(1000.0, parse("1,000"), 1e-9);
        assertEquals(1000.0, parse("1'000"), 1e-9);
        assertEquals(1_234_567.0, parse("1,234,567"), 1e-9);
        assertEquals(1_000_000.0, parse("1 000 000"), 1e-9);
        assertEquals(1000.0, parse("1, 000"), 1e-9);
        assertEquals(1000.0, parse("1,,000"), 1e-9);
        assertEquals(5.0, parse(",5"), 1e-9);
        assertEquals(1.0, parse("1 "), 1e-9);
    }

    @Test
    public void rangesAndTrailingPunctuation() {
        assertEquals(1.0, parse("1-2-3"), 1e-9);
        assertEquals(5.0, parse("5-"), 1e-9);
        assertEquals(100.0, parse("100 - 200"), 1e-9);
        assertEquals(1.5, parse("1.5-2.5M"), 1e-9);
    }

    @Test
    public void europeanDecimalCommaIsMisreadAsThousands() {

        assertEquals(15_000_000.0, parse("1,5M"), 1e-9);
        assertEquals(15_000_000.0, parse("1 5M"), 1e-9);
    }

    @Test
    public void onlyAsciiSpaceIsASeparator_nbspEndsTheNumber() {

        assertEquals(5.0, parse("5 000"), 1e-9);
        assertEquals(5000.0, parse("5 000"), 1e-9);
    }

    @Test
    public void neverThrowsOnAdversarialInput() {

        String[] nasty = { null, "", "-", ".", "k", "M", "١٢٣", "1,,,M", "----",
            "..M", "9".repeat(400), "-".repeat(50) + "5", "1.5 \tMkBx" };
        for (String s : nasty) {
            parse(s);
        }
    }
}
