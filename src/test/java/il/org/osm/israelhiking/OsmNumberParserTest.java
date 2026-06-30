package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Tag("unit")
public class OsmNumberParserTest {

    @ParameterizedTest
    @CsvSource({
        "4302, 4302",
        "'14,115', 14115",
        "'1 000', 1000",
        "14115 ft, 14115",
        "'1,234 m', 1234",
        "'5000 (2011)', 5000",
        "1.234, 1234",
        "'12''345', 12345",
    })
    public void populationParsesToPositiveInt(String raw, int expected) {
        var parsed = OsmNumberParser.parsePopulation(raw);
        assertTrue(parsed.isPresent(), "expected a population for " + raw);
        assertEquals(expected, parsed.getAsInt());
    }

    @ParameterizedTest
    @CsvSource({
        "yes",
        "abc",
        "0",
        "-5",
        "''",
    })
    public void populationRejectsNonPositiveOrNonNumeric(String raw) {
        assertFalse(OsmNumberParser.parsePopulation(raw).isPresent(), "did not expect a population for " + raw);
    }

    @Test
    public void populationRejectsNull() {
        assertFalse(OsmNumberParser.parsePopulation(null).isPresent());
    }

    @ParameterizedTest
    @CsvSource({
        "4302, 4302.0",
        "4302.3, 4302.3",
        "'14,115', 14115.0",
        "'1 000', 1000.0",
        "'1,234 m', 1234.0",
        "-430, -430.0",
        "-430 m, -430.0",
        "'-1,234', -1234.0",
        "100-200, 100.0",
        "- 430, 430.0",
    })
    public void elevationParsesMetres(String raw, double expected) {
        var parsed = OsmNumberParser.parseElevation(raw);
        assertTrue(parsed.isPresent(), "expected an elevation for " + raw);
        assertEquals(expected, parsed.getAsDouble(), 1e-9);
    }

    @ParameterizedTest
    @CsvSource({
        "14115 ft, 4302.252",
        "1000 ft, 304.8",
        "500 feet, 152.4",
        "3000', 914.4",
    })
    public void elevationConvertsFeetToMetres(String raw, double expectedMetres) {
        assertEquals(expectedMetres, OsmNumberParser.parseElevation(raw).getAsDouble(), 1e-3,
                "feet must be converted to metres for " + raw);
    }

    @ParameterizedTest
    @CsvSource({
        "yes",
        "abc",
        "''",
    })
    public void elevationRejectsNonNumeric(String raw) {
        assertFalse(OsmNumberParser.parseElevation(raw).isPresent());
    }

    @Test
    public void elevationRejectsNull() {
        assertFalse(OsmNumberParser.parseElevation(null).isPresent());
    }
}
