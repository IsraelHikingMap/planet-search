package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.onthegomap.planetiler.reader.WithTags;

@Tag("unit")
public class OsmFeatureClassifierTest {

    private static WithTags tags(String... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return WithTags.from(m);
    }

    private static Arguments row(String icon, String color, String category, String... kv) {
        return Arguments.of(icon, color, category, kv);
    }

    static Stream<Arguments> iconGolden() {
        return Stream.of(
            row("icon-leaf", "#008000", "Other", "boundary", "protected_area"),
            row("icon-leaf", "#008000", "Other", "boundary", "national_park"),
            row("icon-leaf", "#008000", "Other", "leisure", "nature_reserve"),
            row("icon-hike", "black", "Hiking", "route", "hiking"),
            row("icon-hike", "black", "Hiking", "route", "foot"),
            row("icon-bike", "black", "Bicycle", "route", "bicycle"),
            row("icon-bike", "black", "Bicycle", "route", "mtb"),
            row("icon-four-by-four", "black", "4x4", "route", "road", "scenic", "yes"),
            row("icon-search", "black", "Other", "route", "road"),
            row("icon-ruins", "#666666", "Historic", "historic", "ruins"),
            row("icon-archaeological", "#666666", "Historic", "historic", "archaeological_site"),
            row("icon-memorial", "#666666", "Historic", "historic", "memorial"),
            row("icon-memorial", "#666666", "Historic", "historic", "monument"),
            row("icon-cave", "black", "Natural", "historic", "tomb"),
            row("icon-search", "black", "Other", "historic", "castle"),
            row("icon-picnic", "#734a08", "Camping", "leisure", "picnic_table"),
            row("icon-picnic", "#734a08", "Camping", "tourism", "picnic_site"),
            row("icon-picnic", "#734a08", "Camping", "amenity", "picnic"),
            row("icon-cave", "black", "Natural", "natural", "cave_entrance"),
            row("icon-tint", "#1e80e3", "Water", "natural", "spring"),
            row("icon-tree", "#008000", "Natural", "natural", "tree"),
            row("icon-flowers", "#008000", "Natural", "natural", "flowers"),
            row("icon-waterhole", "#1e80e3", "Water", "natural", "waterhole"),
            row("icon-peak", "black", "Natural", "natural", "peak"),
            row("icon-peak", "black", "Natural", "natural", "volcano"),
            row("icon-peak", "black", "Natural", "natural", "ridge"),
            row("icon-search", "black", "Other", "natural", "hot_spring"),
            row("icon-search", "black", "Other", "natural", "valley"),
            row("icon-tint", "#1e80e3", "Water", "water", "reservoir"),
            row("icon-tint", "#1e80e3", "Water", "water", "pond"),
            row("icon-tint", "#1e80e3", "Water", "water", "lake"),
            row("icon-tint", "#1e80e3", "Water", "water", "stream_pool"),
            row("icon-water-well", "#1e80e3", "Water", "man_made", "water_well"),
            row("icon-cistern", "#1e80e3", "Water", "man_made", "cistern"),
            row("icon-search", "black", "Other", "man_made", "tower"),
            row("icon-waterfall", "#1e80e3", "Water", "waterway", "waterfall"),
            row("icon-river", "#1e80e3", "Water", "type", "waterway"),
            row("icon-search", "black", "Other", "waterway", "stream"),
            row("icon-home", "black", "Wikipedia", "place", "city"),
            row("icon-home", "black", "Wikipedia", "place", "town"),
            row("icon-home", "black", "Wikipedia", "place", "village"),
            row("icon-home", "black", "Wikipedia", "place", "hamlet"),
            row("icon-home", "black", "Wikipedia", "place", "suburb"),
            row("icon-home", "black", "Wikipedia", "place", ""),
            row("icon-home", "black", "Wikipedia", "place", " "),
            row("icon-viewpoint", "#008000", "Viewpoint", "tourism", "viewpoint"),
            row("icon-campsite", "#734a08", "Camping", "tourism", "camp_site"),
            row("icon-star", "#ffb800", "Other", "tourism", "attraction"),
            row("icon-artwork", "#ffb800", "Other", "tourism", "artwork"),
            row("icon-alpinehut", "#734a08", "Camping", "tourism", "alpine_hut"),
            row("icon-search", "black", "Other", "tourism", "hotel"),
            row("icon-bike", "black", "Bicycle", "highway", "cycleway"),
            row("icon-hike", "black", "Hiking", "highway", "footway"),
            row("icon-hike", "black", "Hiking", "highway", "path"),
            row("icon-four-by-four", "black", "4x4", "highway", "track"),
            row("icon-synagogue", "black", "Other", "amenity", "place_of_worship", "religion", "jewish"),
            row("icon-church", "black", "Other", "amenity", "place_of_worship", "religion", "christian"),
            row("icon-mosque", "black", "Other", "amenity", "place_of_worship", "religion", "muslim"),
            row("icon-holy-place", "black", "Other", "amenity", "place_of_worship"),
            row("icon-holy-place", "black", "Other", "amenity", "monastery"),
            row("icon-inature", "#116C00", "iNature", "ref:IL:inature", "1234"),
            row("icon-search", "black", "Other"),
            row("icon-leaf", "#008000", "Other", "boundary", "national_park", "natural", "peak"),
            row("icon-cave", "black", "Natural", "historic", "tomb", "natural", "spring"),
            row("icon-ruins", "#666666", "Historic", "place", "city", "historic", "ruins"),
            row("icon-peak", "black", "Natural", "natural", "peak", "place", "city"));
    }

    @ParameterizedTest
    @MethodSource("iconGolden")
    public void iconClassificationIsStable(String icon, String color, String category, String[] kv) {
        var c = OsmFeatureClassifier.classify(tags(kv));
        assertEquals(icon, c.icon, "icon for " + Map.of("tags", String.join(",", kv)));
        assertEquals(color, c.color, "color for " + String.join(",", kv));
        assertEquals(category, c.poiCategory, "category for " + String.join(",", kv));
    }

    static Stream<Arguments> baseScoreAnchors() {
        return Stream.of(
            Arguments.of(0.80, new String[] { "boundary", "national_park" }),
            Arguments.of(0.80, new String[] { "boundary", "protected_area" }),
            Arguments.of(0.25, new String[] { "leisure", "nature_reserve" }),
            Arguments.of(1.00, new String[] { "place", "city" }),
            Arguments.of(0.80, new String[] { "place", "town" }),
            Arguments.of(0.55, new String[] { "place", "village" }),
            Arguments.of(0.35, new String[] { "place", "hamlet" }),
            Arguments.of(0.45, new String[] { "place", "suburb" }),
            Arguments.of(0.25, new String[] { "place", "" }),
            Arguments.of(0.55, new String[] { "tourism", "viewpoint" }),
            Arguments.of(0.55, new String[] { "tourism", "attraction" }),
            Arguments.of(0.55, new String[] { "tourism", "alpine_hut" }),
            Arguments.of(0.55, new String[] { "historic", "castle" }),
            Arguments.of(0.55, new String[] { "historic", "ruins" }),
            Arguments.of(0.30, new String[] { "natural", "spring" }),
            Arguments.of(0.30, new String[] { "natural", "cave_entrance" }),
            Arguments.of(0.30, new String[] { "natural", "hot_spring" }),
            Arguments.of(0.30, new String[] { "waterway", "stream" }),
            Arguments.of(0.30, new String[] { "natural", "peak" }),
            Arguments.of(0.30, new String[] { "natural", "volcano" }),
            Arguments.of(0.25, new String[] { "natural", "tree" }),
            Arguments.of(0.25, new String[0]));
    }

    @ParameterizedTest
    @MethodSource("baseScoreAnchors")
    public void baseScoreMatchesLegacy(double expected, String[] kv) {
        assertEquals(expected, OsmFeatureClassifier.classify(tags(kv)).baseScore, 1e-9,
                "baseScore for " + String.join(",", kv));
    }

    static Stream<Arguments> multiTagBaseScoreShifts() {
        return Stream.of(
            Arguments.of(0.55, new String[] { "place", "city", "historic", "ruins" }),
            Arguments.of(0.30, new String[] { "natural", "spring", "place", "city" }),
            Arguments.of(0.80, new String[] { "boundary", "national_park", "place", "city" }),
            Arguments.of(0.30, new String[] { "natural", "peak", "place", "city" }),
            Arguments.of(0.25, new String[] { "leisure", "nature_reserve", "tourism", "viewpoint" }));
    }

    @ParameterizedTest
    @MethodSource("multiTagBaseScoreShifts")
    public void multiTagBaseScoreFollowsDisplayedCategory(double expected, String[] kv) {
        assertEquals(expected, OsmFeatureClassifier.classify(tags(kv)).baseScore, 1e-9,
                "multi-tag base score follows the displayed category for " + String.join(",", kv));
    }

    static Stream<Arguments> nonIconGolden() {
        return Stream.of(
            Arguments.of("icon-search", "black", "Other", new String[] { "amenity", "place_of_worship" }),
            Arguments.of("icon-search", "black", "Other", new String[] { "natural", "valley" }),
            Arguments.of("icon-search", "black", "Other", new String[] { "building", "yes" }),
            Arguments.of("icon-bus-stop", "black", "Other", new String[] { "railway", "station" }),
            Arguments.of("icon-bus-stop", "black", "Other", new String[] { "aerialway", "station" }),
            Arguments.of("icon-peak", "black", "Other", new String[] { "natural", "ridge" }),
            Arguments.of("icon-bike", "green", "Bicycle",
                new String[] { "landuse", "recreation_ground", "sport", "mtb" }),
            Arguments.of("icon-tree", "#008000", "Other", new String[] { "landuse", "forest" }),
            Arguments.of("icon-wikipedia-w", "black", "Wikipedia",
                new String[] { "amenity", "place_of_worship", "wikidata", "Q1" }),
            Arguments.of("icon-wikipedia-w", "black", "Wikipedia",
                new String[] { "building", "yes", "wikipedia", "en:X" }),
            Arguments.of("icon-tree", "#008000", "Other",
                new String[] { "landuse", "forest", "wikidata", "Q1" }),
            Arguments.of("icon-bus-stop", "black", "Other",
                new String[] { "building", "yes", "railway", "station" }));
    }

    @ParameterizedTest
    @MethodSource("nonIconGolden")
    public void nonIconClassificationIsStable(String icon, String color, String category, String[] kv) {
        var c = OsmFeatureClassifier.classifyNonIcon(tags(kv));
        assertEquals(icon, c.icon, "icon for " + String.join(",", kv));
        assertEquals(color, c.color, "color for " + String.join(",", kv));
        assertEquals(category, c.poiCategory, "category for " + String.join(",", kv));
    }

    @ParameterizedTest
    @MethodSource("nonIndexableNonIcon")
    public void nonIndexableFeaturesReturnNull(String[] kv) {
        assertNull(OsmFeatureClassifier.classifyNonIcon(tags(kv)));
    }

    static Stream<Arguments> nonIndexableNonIcon() {
        return Stream.of(
            Arguments.of((Object) new String[0]),
            Arguments.of((Object) new String[] { "building", "no" }),
            Arguments.of((Object) new String[] { "amenity", "restaurant" }));
    }

    @Test
    public void historicNonIconFeatureScoresViaHistoricFallbackNotItsDisplayCategory() {
        WithTags historicBuilding = tags("building", "yes", "historic", "manor", "name", "Old Manor");

        OsmFeatureClassifier.Category display = OsmFeatureClassifier.classifyNonIcon(historicBuilding);
        OsmFeatureClassifier.Category scoring = OsmFeatureClassifier.classify(historicBuilding);

        assertEquals(OsmFeatureClassifier.Category.NONICON_GENERIC, display);
        assertEquals(OsmFeatureClassifier.Category.FALLBACK_HISTORIC, scoring);
        assertTrue(scoring.baseScore > display.baseScore,
                "a historic non-icon feature is scored via the historic fallback " + scoring.baseScore
                        + ", outranking a plain non-icon's " + display.baseScore);
    }
}
