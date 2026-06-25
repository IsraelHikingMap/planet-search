package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Pins the doubled-only Hebrew matres fold: collapse doubled vav/yod, leave single letters intact. */
@Tag("unit")
public class HebrewMatresRuleTest {

    @Test
    public void collapsesDoubledVav() {

        assertEquals("דויד", ElasticsearchHelper.applyHebrewMatresDoubledOnly("דוויד"),
                "a doubled vav (וו) must collapse to a single vav (ו)");
    }

    @Test
    public void collapsesDoubledYod() {

        assertEquals("בילי", ElasticsearchHelper.applyHebrewMatresDoubledOnly("ביילי"),
                "a doubled yod (יי) must collapse to a single yod (י)");
    }

    @Test
    public void collapsesDoubledAtWordStart() {
        assertEquals("ויקי", ElasticsearchHelper.applyHebrewMatresDoubledOnly("וויקי"),
                "doubled vav at the start must also collapse");
    }

    @Test
    public void leavesSingleVavUntouched() {

        assertEquals("אור", ElasticsearchHelper.applyHebrewMatresDoubledOnly("אור"),
                "a single vav must be left intact (homograph-safety: אור must NOT become אר)");
    }

    @Test
    public void leavesSingleYodUntouched() {
        assertEquals("עיר", ElasticsearchHelper.applyHebrewMatresDoubledOnly("עיר"),
                "a single yod must be left intact (עיר must NOT become ער)");
    }

    @Test
    public void doesNotFixTheClientDefectiveSpelling() {

        var full = "אופניים";
        var defective = "אפניים";
        var foldedFull = ElasticsearchHelper.applyHebrewMatresDoubledOnly(full);
        var foldedDefective = ElasticsearchHelper.applyHebrewMatresDoubledOnly(defective);
        org.junit.jupiter.api.Assertions.assertNotEquals(foldedFull, foldedDefective,
                "doubled-only does NOT make full and defective spellings collide (HV01/HV02 unfixed) "
                        + "— the missing-vav gap survives; only the deferred interior-vav rule closes it");
    }

    @Test
    public void nullIsNull() {
        assertEquals(null, ElasticsearchHelper.applyHebrewMatresDoubledOnly(null));
    }
}
