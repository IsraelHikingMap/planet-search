package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the OSM-tag -> feature_class classifier (classifyFeatureClass). Pins the landform
 * vocabulary expansion (canyon/gorge/mesa/plateau/arch/cave/wetland and the
 * arete/crater/mountain_range folds) and guards the pre-existing mappings against regression.
 * Doc-side classes here must each have a class_groups entry in updated_score.yml, or the query-time
 * pclass boost is a silent no-op.
 */
@Tag("unit")
public class SetFeatureClassTest {

    private static Function<String, String> tags(Map<String, String> m) {
        return m::get;
    }

    private static String classify(Map<String, String> m) {
        return OsmTagUtils.classifyFeatureClass(tags(m));
    }

    // --- new landform classes ------------------------------------------------

    @Test
    public void canyonAndGorgeMapToCanyon() {
        assertEquals("canyon", classify(Map.of("natural", "canyon")));
        assertEquals("canyon", classify(Map.of("natural", "gorge")));
    }

    @Test
    public void mesaAndPlateauMapToPlateau() {
        assertEquals("plateau", classify(Map.of("natural", "mesa")));
        assertEquals("plateau", classify(Map.of("natural", "plateau")));
    }

    @Test
    public void archCaveAndWetlandGetTheirOwnClass() {
        assertEquals("arch", classify(Map.of("natural", "arch")));
        assertEquals("cave", classify(Map.of("natural", "cave_entrance")));
        assertEquals("wetland", classify(Map.of("natural", "wetland")));
    }

    @Test
    public void areteCraterAndRangeFoldToExistingClasses() {
        assertEquals("ridge", classify(Map.of("natural", "arete")));
        assertEquals("peak", classify(Map.of("natural", "crater")));
        assertEquals("peak", classify(Map.of("natural", "mountain_range")));
    }

    // --- regression guards on pre-existing mappings --------------------------

    @Test
    public void valleyIsUnchanged() {
        assertEquals("valley", classify(Map.of("natural", "valley")));
    }

    @Test
    public void peakVolcanoAndWaterRefinementUnchanged() {
        assertEquals("peak", classify(Map.of("natural", "peak")));
        assertEquals("peak", classify(Map.of("natural", "volcano")));
        assertEquals("reservoir", classify(Map.of("natural", "water", "water", "reservoir")));
        assertEquals("lake", classify(Map.of("natural", "water")));
    }

    @Test
    public void waterRiverAndCanalAreNotMislabelledAsLake() {
        // natural=water + water=river|canal are common (wide-river / canal surfaces); they must not
        // be classified as lakes. Unknown sub-types get the neutral "water", never "lake".
        assertEquals("river", classify(Map.of("natural", "water", "water", "river")));
        assertEquals("canal", classify(Map.of("natural", "water", "water", "canal")));
        assertEquals("lagoon", classify(Map.of("natural", "water", "water", "lagoon")));
        assertEquals("water", classify(Map.of("natural", "water", "water", "oxbow")));
        // lake/reservoir/pond and the no-subtype base case stay as before.
        assertEquals("lake", classify(Map.of("natural", "water", "water", "lake")));
        assertEquals("reservoir", classify(Map.of("natural", "water", "water", "reservoir")));
        assertEquals("pond", classify(Map.of("natural", "water", "water", "pond")));
        assertEquals("lake", classify(Map.of("natural", "water")));
    }

    @Test
    public void unrecognisedNaturalFallsBackToNaturalCatchAll() {
        // sand/dune/basin/... deliberately stay in the generic catch-all (OUTDOOR_GENERIC group).
        assertEquals("natural", classify(Map.of("natural", "sand")));
    }

    // --- non-natural primary tags --------------------------------------------

    @Test
    public void railwayAndLanduseStayNull() {
        // railway/landuse carry no classified primary type tag, so they stay null (no spurious
        // class). Note: railway=station is intentionally NOT classified — it reaches the index via
        // a poiIcon path, but feature_class folds station handling under building=train_station.
        assertNull(classify(Map.of("railway", "station")));
        assertNull(classify(Map.of("landuse", "forest")));
    }

    // --- built / POI classes --------------------------------------

    @Test
    public void lodgingTourismMapsToLodging() {
        assertEquals("lodging", classify(Map.of("tourism", "hotel")));
        assertEquals("lodging", classify(Map.of("tourism", "guest_house")));
        assertEquals("museum", classify(Map.of("tourism", "museum")));
        // pre-existing tourism mappings unchanged
        assertEquals("viewpoint", classify(Map.of("tourism", "viewpoint")));
        assertEquals("campsite", classify(Map.of("tourism", "camp_site")));
    }

    @Test
    public void amenityMapsToPoiClasses() {
        assertEquals("food", classify(Map.of("amenity", "restaurant")));
        assertEquals("food", classify(Map.of("amenity", "cafe")));
        assertEquals("religious", classify(Map.of("amenity", "place_of_worship")));
        assertEquals("education", classify(Map.of("amenity", "school")));
        assertEquals("medical", classify(Map.of("amenity", "pharmacy")));
        assertEquals("government", classify(Map.of("amenity", "townhall")));
        assertEquals("parking", classify(Map.of("amenity", "parking")));
        assertEquals("fuel", classify(Map.of("amenity", "fuel")));
        // unrecognised amenity falls back to the amenity catch-all (BUILT_GENERIC group)
        assertEquals("amenity", classify(Map.of("amenity", "bench")));
    }

    @Test
    public void leisureShopOfficeHealthcareMapped() {
        assertEquals("park", classify(Map.of("leisure", "park")));
        assertEquals("park", classify(Map.of("leisure", "nature_reserve")));
        assertEquals("sports", classify(Map.of("leisure", "sports_centre")));
        assertEquals("shop", classify(Map.of("shop", "supermarket")));
        assertEquals("office", classify(Map.of("office", "lawyer")));
        assertEquals("medical", classify(Map.of("healthcare", "clinic")));
    }

    @Test
    public void manMadeAndBuildingMapped() {
        assertEquals("structure", classify(Map.of("man_made", "lighthouse")));
        assertEquals("man_made", classify(Map.of("man_made", "antenna")));
        assertEquals("religious", classify(Map.of("building", "church")));
        assertEquals("transit", classify(Map.of("building", "train_station")));
        // a named generic building gets the generic class, not null
        assertEquals("building", classify(Map.of("building", "yes")));
        assertEquals("building", classify(Map.of("building", "hall")));
        // building=no/none never classifies
        assertNull(classify(Map.of("building", "no")));
        assertNull(classify(Map.of("building", "none")));
    }

    @Test
    public void outdoorWinsOverBuiltTags() {
        // priority order: natural > waterway > place > historic > tourism > built/POI keys.
        // A viewpoint that is also a building stays a viewpoint; a peak that is also amenity stays peak.
        assertEquals("viewpoint", classify(Map.of("tourism", "viewpoint", "building", "yes")));
        assertEquals("peak", classify(Map.of("natural", "peak", "amenity", "restaurant")));
        assertEquals("lake", classify(Map.of("natural", "water", "leisure", "park")));
    }

    @Test
    public void naturalWinsOverOtherPrimaryTags() {
        // priority order: natural > waterway > place > historic > tourism
        assertEquals("canyon", classify(Map.of("natural", "canyon", "place", "locality")));
    }

    @Test
    public void noTagsYieldsNull() {
        assertNull(classify(Map.of("name", "Anonymous")));
    }
}
