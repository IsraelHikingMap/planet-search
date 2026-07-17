package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.onthegomap.planetiler.reader.WithTags;

@Tag("unit")
public class OsmNamesTest {

    private static WithTags tags(String... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return WithTags.from(m);
    }

    @Test
    public void hasSearchableName_matchesDefaultName() {
        assertTrue(OsmNames.hasSearchableName(tags("name", "X"), new String[] {}));
    }

    @Test
    public void hasSearchableName_matchesASupportedLanguage() {
        assertTrue(OsmNames.hasSearchableName(tags("name:en", "X"), new String[] { "en" }));
    }

    @Test
    public void hasSearchableName_ignoresUnsupportedLanguages() {
        assertFalse(OsmNames.hasSearchableName(tags("name:en", "X"), new String[] { "he" }));
    }

    @Test
    public void hasSearchableName_falseWhenThereIsNoName() {
        assertFalse(OsmNames.hasSearchableName(tags("place", "city"), new String[] { "en" }));
    }

    @Test
    public void alternativeNames_readsUnsuffixedTagsForTheDefaultLanguage() {
        var feature = tags("alt_name", "A", "loc_name", "B", "official_name", "C");
        assertEquals(List.of("A", "B", "C"), OsmNames.alternativeNames(feature, "default"));
    }

    @Test
    public void alternativeNames_splitsTrimsAndDeduplicates() {
        var feature = tags("alt_name", "A ; B ; A", "old_name", "B");
        assertEquals(List.of("A", "B"), OsmNames.alternativeNames(feature, "default"));
    }

    @Test
    public void alternativeNames_readsTheLanguageSuffixedTags() {
        var feature = tags("alt_name", "Def", "alt_name:en", "En");
        assertEquals(List.of("En"), OsmNames.alternativeNames(feature, "en"));
        assertEquals(List.of(), OsmNames.alternativeNames(feature, "he"));
    }

    @Test
    public void isNameOrDescriptionTag_recognizesNameDescriptionAndAltNameTags() {
        assertTrue(OsmNames.isNameOrDescriptionTag("name:en"));
        assertTrue(OsmNames.isNameOrDescriptionTag("description:fr"));
        assertTrue(OsmNames.isNameOrDescriptionTag("alt_name"));
        assertTrue(OsmNames.isNameOrDescriptionTag("loc_name:he"));
        assertFalse(OsmNames.isNameOrDescriptionTag("boundary"));
        assertFalse(OsmNames.isNameOrDescriptionTag("name"));
    }
}
