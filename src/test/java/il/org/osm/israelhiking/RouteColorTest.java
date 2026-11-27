package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.onthegomap.planetiler.reader.WithTags;

class RouteColorTest {

  @Test
  void usesColourTag() {
    var route = WithTags.from(Map.of("colour", "red", "osmc:symbol", "blue:white:blue_bar"));
    assertEquals("red", RouteColor.of(route, Map.of("default", "Trail"), 1));
  }

  @Test
  void fallsBackToOsmcSymbolWayColor() {
    var route = WithTags.from(Map.of("osmc:symbol", "blue:white:blue_bar"));
    assertEquals("blue", RouteColor.of(route, Map.of("default", "Trail"), 1));
  }

  @Test
  void hashesTheNameWhenThereIsNoColor() {
    var route = WithTags.from(Map.of());
    var color = RouteColor.of(route, Map.of("default", "Israel National Trail"), 1);
    assertTrue(color.matches("#[0-9a-f]{6}"), color);
    assertEquals(color, RouteColor.of(route, Map.of("default", "Israel National Trail", "en", "x"), 2));
    assertNotEquals(color, RouteColor.of(route, Map.of("default", "Jesus Trail"), 1));
  }

  @Test
  void hashesTheIdWhenThereIsNoName() {
    var route = WithTags.from(Map.of());
    assertEquals(RouteColor.of(route, Map.of("default", "42"), 1), RouteColor.of(route, Map.of(), 42));
  }
}
