package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the Hebrew matres-lectionis DOUBLED-ONLY rule that the he-scoped char_filters
 * (hebrew_matres / hebrew_matres_yod) apply. We exercise the pure reference implementation
 * ElasticsearchHelper.applyHebrewMatresDoubledOnly (same rule the index char_filters encode) so the
 * doubled-vs-single behaviour is pinned without a live cluster.
 *
 * Scope: doubled-only is the safe, ~zero-homograph-break rule. It does NOT fix the client's
 * אופניים/אפניים case — that needs the deferred single-interior-vav rule, which breaks ~7 real
 * homographs (אור/אר, דוד/דד, ...) and is parked for client sign-off. These tests assert exactly
 * that boundary.
 */
@Tag("unit")
public class HebrewMatresRuleTest {

    @Test
    public void collapsesDoubledVav() {
        // וו -> ו  (the matres-doubled-vav case the rule is FOR)
        assertEquals("דויד", ElasticsearchHelper.applyHebrewMatresDoubledOnly("דוויד"),
                "a doubled vav (וו) must collapse to a single vav (ו)");
    }

    @Test
    public void collapsesDoubledYod() {
        // יי -> י
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
        // The deliberately-conservative part: a SINGLE interior vav must NOT be dropped.
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
        // Honest boundary: the client's defective spelling אפניים differs
        // from the full אופניים by a MISSING interior vav — NOT a doubled letter. The doubled-only
        // rule does collapse the doubled YOD that both forms share (אופניים->אופנים, אפניים->אפנים),
        // but it leaves the vav difference intact, so the two forms STILL do not collide. Fixing
        // HV01/HV02 needs the deferred single-interior-vav rule (which breaks ~7 homographs).
        var full = "אופניים";     // full spelling (has the vav)
        var defective = "אפניים"; // client's defective spelling (no vav)
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
