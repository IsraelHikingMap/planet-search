package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Pins buildAltNames: per-language and default alt-name collection from the alias tag family. */
@Tag("unit")
public class AddAltNamesTest {

    private static final String[] LANGS = { "en", "he", "ru", "ar", "es" };

    private static Function<String, String> tags(Map<String, String> m) {
        return m::get;
    }

    @Test
    public void splitsSemicolonSeparatedMultiValues() {

        var altNames = OsmTagUtils.buildAltNames(LANGS,
                tags(Map.of("alt_name", "Foo;Bar;Baz")));
        assertEquals(List.of("Foo", "Bar", "Baz"), altNames.get("default"),
                "alt_name must be split on ';' and all variants kept");
    }

    @Test
    public void trimsWhitespaceAndDropsEmptyParts() {
        var altNames = OsmTagUtils.buildAltNames(LANGS,
                tags(Map.of("alt_name", " Foo ;; Bar ;  ")));
        assertEquals(List.of("Foo", "Bar"), altNames.get("default"),
                "parts must be trimmed and empty parts dropped");
    }

    @Test
    public void deDuplicatesWhileKeepingOrder() {
        var map = new HashMap<String, String>();
        map.put("alt_name", "Foo;Bar");
        map.put("official_name", "Bar;Qux");
        var altNames = OsmTagUtils.buildAltNames(LANGS, tags(map));
        assertEquals(List.of("Foo", "Bar", "Qux"), altNames.get("default"),
                "duplicate variants across tags must be collapsed, insertion order preserved");
    }

    @Test
    public void excludesOldName() {

        var altNames = OsmTagUtils.buildAltNames(LANGS,
                tags(Map.of("old_name", "Former Name")));
        assertNull(altNames, "old_name must be excluded entirely (no alt_names produced)");
    }

    @Test
    public void includesOfficialShortLocAndIntName() {
        var map = new HashMap<String, String>();
        map.put("official_name", "Official");
        map.put("short_name", "Sh");
        map.put("loc_name", "Local");
        map.put("int_name", "International");
        var altNames = OsmTagUtils.buildAltNames(LANGS, tags(map));
        var def = altNames.get("default");
        assertTrue(def.contains("Official"), "official_name must be indexed");
        assertTrue(def.contains("Sh"), "short_name must be indexed");
        assertTrue(def.contains("Local"), "loc_name must be indexed");
        assertTrue(def.contains("International"), "int_name must be indexed");
    }

    @Test
    public void languageSuffixedTagsGoUnderTheirLanguageKey() {
        var map = new HashMap<String, String>();
        map.put("alt_name:he", "המיסדים;יונתן");
        map.put("alt_name:en", "IBT");
        var altNames = OsmTagUtils.buildAltNames(LANGS, tags(map));
        assertEquals(List.of("המיסדים", "יונתן"), altNames.get("he"), "alt_name:he must key under 'he', split");
        assertEquals(List.of("IBT"), altNames.get("en"), "alt_name:en must key under 'en'");
        assertFalse(altNames.containsKey("default"), "no bare alt_name => no 'default' key");
    }

    @Test
    public void theIbtCase_altNameEnIbtIsSearchable() {

        var altNames = OsmTagUtils.buildAltNames(LANGS,
                tags(Map.of("alt_name:en", "IBT")));
        assertEquals(List.of("IBT"), altNames.get("en"));
    }

    @Test
    public void languageAndDefaultTagsCoexist() {

        var map = new HashMap<String, String>();
        map.put("alt_name:he", "המיסדים");
        map.put("alt_name", "Founders");
        var altNames = OsmTagUtils.buildAltNames(LANGS, tags(map));
        assertEquals(List.of("המיסדים"), altNames.get("he"));
        assertEquals(List.of("Founders"), altNames.get("default"),
                "the bare alt_name must still be added under 'default' alongside the language key");
    }

    @Test
    public void emptyTagValueIsIgnored() {

        var altNames = OsmTagUtils.buildAltNames(LANGS, tags(Map.of("alt_name", "")));
        assertNull(altNames, "an empty alt_name value must produce no alt_names");
    }

    @Test
    public void noVariantTagsYieldsNull() {

        var altNames = OsmTagUtils.buildAltNames(LANGS,
                tags(Map.of("name", "Just A Name", "old_name", "stale")));
        assertNull(altNames, "a feature with no variant tags must produce no alt_names");
    }
}
