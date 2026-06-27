package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class ParseFirstNumberTest {

    @Test
    public void plainInteger() {
        assertEquals(4302.0, PlanetSearchProfile.parseFirstNumber("4302"), 1e-9);
    }

    @Test
    public void decimal() {
        assertEquals(4302.3, PlanetSearchProfile.parseFirstNumber("4302.3"), 1e-9);
    }

    @Test
    public void thousandsSeparatorComma() {
        assertEquals(14115.0, PlanetSearchProfile.parseFirstNumber("14,115"), 1e-9);
    }

    @Test
    public void thousandsSeparatorSpace() {
        assertEquals(1000.0, PlanetSearchProfile.parseFirstNumber("1 000"), 1e-9);
    }

    @Test
    public void numberWithUnitSuffix() {
        assertEquals(14115.0, PlanetSearchProfile.parseFirstNumber("14115 ft"), 1e-9);
    }

    @Test
    public void yesIsNotANumber() {
        assertTrue(Double.isNaN(PlanetSearchProfile.parseFirstNumber("yes")));
    }

    @Test
    public void nullIsNaN() {
        assertTrue(Double.isNaN(PlanetSearchProfile.parseFirstNumber(null)));
    }

    @Test
    public void emptyIsNaN() {
        assertTrue(Double.isNaN(PlanetSearchProfile.parseFirstNumber("")));
    }

    @Test
    public void rangeTakesFirstNumber() {
        assertEquals(100.0, PlanetSearchProfile.parseFirstNumber("100-200"), 1e-9);
    }

    @Test
    public void pureNonNumericIsNaN() {
        assertTrue(Double.isNaN(PlanetSearchProfile.parseFirstNumber("abc")));
    }

    @Test
    public void commaThousandsWithUnitSuffix() {
        assertEquals(1234.0, PlanetSearchProfile.parseFirstNumber("1,234 m"), 1e-9);
    }

    @Test
    public void negativeInteger() {
        assertEquals(-430.0, PlanetSearchProfile.parseFirstNumber("-430"), 1e-9);
    }

    @Test
    public void negativeWithUnitSuffix() {
        assertEquals(-430.0, PlanetSearchProfile.parseFirstNumber("-430 m"), 1e-9);
    }

    @Test
    public void negativeWithThousandsSeparator() {
        assertEquals(-1234.0, PlanetSearchProfile.parseFirstNumber("-1,234"), 1e-9);
    }

    @Test
    public void rangeSeparatorIsNotASign() {
        assertEquals(100.0, PlanetSearchProfile.parseFirstNumber("100-200"), 1e-9);
    }

    @Test
    public void strayLeadingDashIsNotASign() {
        assertEquals(430.0, PlanetSearchProfile.parseFirstNumber("- 430"), 1e-9);
    }
}
