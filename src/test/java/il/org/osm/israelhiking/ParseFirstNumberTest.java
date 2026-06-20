package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Defensive parsing of free-text OSM ele/population values. Must never throw; returns NaN when there
 * is no usable number. A glued magnitude multiplier (k/K=1e3, M=1e6, B=1e9) rescales the digits.
 */
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
        // "100-200" -> 100 (the hyphen ends the first number)
        assertEquals(100.0, parse("100-200"), 1e-9);
    }

    @Test
    public void secondDotEndsTheNumber() {
        // A version-like "1.2.3": the first dot is the decimal point; the second dot (seenDot already
        // true) is non-numeric and ends the number -> 1.2. Pins the seenDot guard's false arm.
        assertEquals(1.2, parse("1.2.3"), 1e-9);
        assertEquals(192.168, parse("192.168.0.1"), 1e-9);
    }

    @Test
    public void pureNonNumericIsNaN() {
        assertTrue(Double.isNaN(parse("abc")));
    }

    @Test
    public void commaThousandsWithUnitSuffix() {
        // "1,234 m" -> 1234.0 (comma stripped as thousands sep, " m" ends the number).
        assertEquals(1234.0, parse("1,234 m"), 1e-9);
    }

    @Test
    public void negativeElevation() {
        // Below-sea-level elevations are real (Dead Sea region) — a leading '-' must be honored.
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

    // --- glued magnitude multipliers (the feature) --------------------------------

    @Test
    public void gluedMagnitudeSuffixMultiplies() {
        // k/K=1e3, M=1e6, B=1e9. So population="1.5M" stores 1_500_000, not 1.
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
        // The first multiplier terminates the number; anything after it is discarded.
        assertEquals(50_000.0, parse("50kk"), 1e-9);    // multiply once, stop
        assertEquals(1_000_000.0, parse("1MM"), 1e-9);  // second M ignored
        assertEquals(1_500.0, parse("1.5km"), 1e-9);    // 'k'->x1000, trailing 'm' ignored
        assertEquals(1_500_000.0, parse("1.5M5"), 1e-9);
        assertEquals(50_000.0, parse("50K100"), 1e-9);
        assertEquals(1_000_000.0, parse("1.M"), 1e-9);  // "1." -> 1.0, then M
    }

    @Test
    public void multiplierBeforeDigitsIsIgnored() {
        // A magnitude letter is only a multiplier when glued AFTER a digit; leading ones are ignored.
        assertEquals(50.0, parse("M50"), 1e-9);
        assertEquals(50.0, parse("k50"), 1e-9);
    }

    @Test
    public void onlyKkMbAreMultipliers_otherLettersAreTrailingUnits() {
        assertEquals(1.5, parse("1.5G"), 1e-9);          // 'G' not in the set
        assertEquals(1.0, parse("1b"), 1e-9);            // lowercase 'b' not recognized (only 'B')
        assertEquals(1.0, parse("1g"), 1e-9);
        assertEquals(1_000_000.0, parse("1Mb"), 1e-9);   // 'M' multiplies, trailing 'b' ignored
    }

    @Test
    public void spaceSeparatedMagnitudeLetterDoesNotMultiply() {
        // A magnitude letter after a separator/space/tab is a trailing token, not a glued suffix.
        assertEquals(1.5, parse("1.5 M"), 1e-9);
        assertEquals(1.5, parse("1.5 km"), 1e-9);
        assertEquals(1.5, parse("1.5\tM"), 1e-9);
    }

    // --- lowercase 'm' is metres, never mega (protects elevations) ----------------

    @Test
    public void gluedMetresUnitStillParses() {
        assertEquals(1500.0, parse("1500m"), 1e-9);
        assertEquals(4302.0, parse("4302m"), 1e-9);
        assertEquals(5.0, parse("5m"), 1e-9);
        assertEquals(100.0, parse("100m2"), 1e-9);   // trailing unit + stray digit ignored
        assertEquals(5.0, parse("5mi"), 1e-9);
        assertEquals(1.5, parse("1.5mm"), 1e-9);
    }

    @Test
    public void unitSeparatedBySpaceStillParses() {
        assertEquals(14115.0, parse("14,115 ft"), 1e-9);
        assertEquals(1200.0, parse("1200 m"), 1e-9);
    }

    // --- characterization: pin the exact behavior of unusual inputs ---------------
    // Values captured by running the real parser. Some are deliberate quirks, others
    // known limitations — each is labelled — so a future change can't silently shift them.

    @ParameterizedTest(name = "\"{0}\" -> NaN")
    @ValueSource(strings = {
        " ", "  ", "--", "- ", "no", ".", "..", "-.", "k", "M", "kg",
        "١٢٣"  // Arabic-Indic digits ١٢٣ — only ASCII 0-9 are recognized
    })
    public void variousNonNumbersAreNaN(String input) {
        assertTrue(Double.isNaN(parse(input)), "expected NaN for \"" + input + "\"");
    }

    @Test
    public void leadingZerosAndDotQuirks() {
        assertEquals(0.0, parse("0"), 1e-9);
        assertEquals(0.0, parse("00"), 1e-9);
        assertEquals(7.0, parse("007"), 1e-9);   // leading zeros dropped by Double.parseDouble
        assertEquals(5.0, parse("5."), 1e-9);    // trailing dot is harmless
        assertEquals(5.0, parse(".5"), 1e-9);    // QUIRK: leading dot dropped, so ".5" == 5, NOT 0.5
        assertEquals(0.0, parse("-0"), 1e-9);    // "-0" -> -0.0 (equal to 0.0 within delta)
    }

    @Test
    public void separatorRunsAreSwallowed() {
        assertEquals(1000.0, parse("1,000"), 1e-9);
        assertEquals(1000.0, parse("1'000"), 1e-9);
        assertEquals(1_234_567.0, parse("1,234,567"), 1e-9);
        assertEquals(1_000_000.0, parse("1 000 000"), 1e-9);
        assertEquals(1000.0, parse("1, 000"), 1e-9);   // comma+space run both swallowed
        assertEquals(1000.0, parse("1,,000"), 1e-9);   // doubled comma swallowed
        assertEquals(5.0, parse(",5"), 1e-9);          // leading comma (no digit yet) ignored
        assertEquals(1.0, parse("1 "), 1e-9);
    }

    @Test
    public void rangesAndTrailingPunctuation() {
        assertEquals(1.0, parse("1-2-3"), 1e-9);
        assertEquals(5.0, parse("5-"), 1e-9);
        assertEquals(100.0, parse("100 - 200"), 1e-9);
        assertEquals(1.5, parse("1.5-2.5M"), 1e-9);   // '-' ends the number before M applies
    }

    @Test
    public void europeanDecimalCommaIsMisreadAsThousands() {
        // KNOWN LIMITATION: comma is a thousands separator, so a European decimal "1,5M" becomes
        // 15 then x1e6 = 15,000,000 (not 1,500,000). Pinned so the behavior is explicit.
        assertEquals(15_000_000.0, parse("1,5M"), 1e-9);
        assertEquals(15_000_000.0, parse("1 5M"), 1e-9);
    }

    @Test
    public void onlyAsciiSpaceIsASeparator_nbspEndsTheNumber() {
        // QUIRK: the separator test matches the ASCII space ' ' only. A non-breaking space (U+00A0)
        // is NOT a separator — it ends the number, so the digits after it are lost.
        assertEquals(5.0, parse("5 000"), 1e-9);   // NBSP -> only "5"
        assertEquals(5000.0, parse("5 000"), 1e-9);     // contrast: ASCII space groups thousands
    }

    @Test
    public void neverThrowsOnAdversarialInput() {
        // The hard contract: parseFirstNumber must NEVER throw, whatever the input. (A huge digit
        // run may parse to Infinity — that's a valid non-throwing result, not an error.)
        String[] nasty = { null, "", "-", ".", "k", "M", "١٢٣", "1,,,M", "----",
            "..M", "9".repeat(400), "-".repeat(50) + "5", "1.5 \tMkBx" };
        for (String s : nasty) {
            parse(s); // reaching the next line means it did not throw
        }
    }
}
