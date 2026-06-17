package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for OsmTagUtils.computePopulation (the place/admin population signal). Pins the two
 * hardening fixes:
 *   - a garbage population tag that overflows to +Infinity must NOT clamp to Integer.MAX_VALUE
 *     (isFinite guard), and must fall through to the settlement ladder;
 *   - a non-settlement place value must return null (no fabricated fallback), so ES uses missing:1.0.
 */
@Tag("unit")
public class ComputePopulationTest {

  // --- POIs (no place tag) -------------------------------------------------

  @Test
  public void nullPlaceYieldsNull() {
    assertNull(OsmTagUtils.computePopulation(null, "12345"));
    assertNull(OsmTagUtils.computePopulation(null, null));
  }

  // --- real population tag wins, clamped ------------------------------------

  @Test
  public void finitePositiveTagWins() {
    assertEquals(12_345, OsmTagUtils.computePopulation("city", "12345"));
    // free-text forms parseFirstNumber handles
    assertEquals(1_500_000, OsmTagUtils.computePopulation("city", "1.5M"));
    assertEquals(50_000, OsmTagUtils.computePopulation("village", "50k"));
  }

  @Test
  public void tagOverridesLadder() {
    // an explicit value beats the village ladder default (2_000)
    assertEquals(9_001, OsmTagUtils.computePopulation("village", "9001"));
  }

  @Test
  public void hugeButFiniteRealValueClampsToMaxInt() {
    // 3e9 is finite and > Integer.MAX_VALUE -> clamp, NOT overflow to a garbage/negative int
    assertEquals(Integer.MAX_VALUE, OsmTagUtils.computePopulation("city", "3000000000"));
    assertEquals(Integer.MAX_VALUE, OsmTagUtils.computePopulation("city", "3B"));
  }

  // --- fix #3: +Infinity garbage tag must fall through to the ladder --------

  @Test
  public void infinityGarbageTagFallsThroughToLadder() {
    // a several-hundred-digit run parses to +Infinity; isFinite rejects it, so a town gets the
    // ladder value (50_000), NOT Integer.MAX_VALUE.
    String huge = "9".repeat(400);
    assertEquals(50_000, OsmTagUtils.computePopulation("town", huge));
  }

  @Test
  public void infinityGarbageTagOnNonSettlementYieldsNull() {
    String huge = "9".repeat(400);
    assertNull(OsmTagUtils.computePopulation("island", huge));
  }

  // --- settlement ladder fallback (missing/invalid tag) ---------------------

  @Test
  public void ladderFallbackWhenTagMissing() {
    assertEquals(1_000_000, OsmTagUtils.computePopulation("city", null));
    assertEquals(50_000, OsmTagUtils.computePopulation("town", null));
    assertEquals(2_000, OsmTagUtils.computePopulation("village", null));
    assertEquals(200, OsmTagUtils.computePopulation("hamlet", null));
  }

  @Test
  public void ladderFallbackWhenTagNonNumeric() {
    assertEquals(2_000, OsmTagUtils.computePopulation("village", "yes"));
    assertEquals(200, OsmTagUtils.computePopulation("hamlet", "unknown"));
  }

  @Test
  public void zeroAndNegativeTagFallThroughToLadder() {
    assertEquals(2_000, OsmTagUtils.computePopulation("village", "0"));
    assertEquals(2_000, OsmTagUtils.computePopulation("village", "-5"));
  }

  // --- fix #4: non-settlement place values yield null (no fabricated 20) -----

  @Test
  public void nonSettlementPlaceYieldsNull() {
    assertNull(OsmTagUtils.computePopulation("island", null));
    assertNull(OsmTagUtils.computePopulation("islet", null));
    assertNull(OsmTagUtils.computePopulation("locality", null));
    assertNull(OsmTagUtils.computePopulation("suburb", null));
    assertNull(OsmTagUtils.computePopulation("neighbourhood", null));
    assertNull(OsmTagUtils.computePopulation("sea", null));
    assertNull(OsmTagUtils.computePopulation("ocean", null));
  }

  @Test
  public void nonSettlementPlaceWithRealTagStillUsesTag() {
    // a locality that does carry a real population still gets it (the tag branch precedes the ladder)
    assertEquals(1_500, OsmTagUtils.computePopulation("locality", "1500"));
  }
}
