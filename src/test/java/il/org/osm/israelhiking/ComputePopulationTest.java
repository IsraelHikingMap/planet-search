package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Pins computePopulation: parsed population, place-class fallbacks, and null for non-places. */
@Tag("unit")
public class ComputePopulationTest {

  @Test
  public void nullPlaceYieldsNull() {
    assertNull(OsmTagUtils.computePopulation(null, "12345"));
    assertNull(OsmTagUtils.computePopulation(null, null));
  }

  @Test
  public void finitePositiveTagWins() {
    assertEquals(12_345, OsmTagUtils.computePopulation("city", "12345"));

    assertEquals(1_500_000, OsmTagUtils.computePopulation("city", "1.5M"));
    assertEquals(50_000, OsmTagUtils.computePopulation("village", "50k"));
  }

  @Test
  public void tagOverridesLadder() {

    assertEquals(9_001, OsmTagUtils.computePopulation("village", "9001"));
  }

  @Test
  public void hugeButFiniteRealValueClampsToMaxInt() {

    assertEquals(Integer.MAX_VALUE, OsmTagUtils.computePopulation("city", "3000000000"));
    assertEquals(Integer.MAX_VALUE, OsmTagUtils.computePopulation("city", "3B"));
  }

  @Test
  public void infinityGarbageTagFallsThroughToLadder() {

    String huge = "9".repeat(400);
    assertEquals(50_000, OsmTagUtils.computePopulation("town", huge));
  }

  @Test
  public void infinityGarbageTagOnNonSettlementYieldsNull() {
    String huge = "9".repeat(400);
    assertNull(OsmTagUtils.computePopulation("island", huge));
  }

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

    assertEquals(1_500, OsmTagUtils.computePopulation("locality", "1500"));
  }
}
