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

    @Test
    public void nullTagLookupResultYieldsNull() {
        // Every primary key absent -> classifier returns null (the absent-feature guard).
        assertNull(OsmTagUtils.classifyFeatureClass(t -> null));
    }

    // --- remaining natural sub-types (one assertion per uncovered switch arm) -

    @Test
    public void naturalLandformArmsClassified() {
        assertEquals("hill", classify(Map.of("natural", "hill")));
        assertEquals("ridge", classify(Map.of("natural", "ridge")));
        assertEquals("saddle", classify(Map.of("natural", "saddle")));
        assertEquals("saddle", classify(Map.of("natural", "gap")));
        assertEquals("cliff", classify(Map.of("natural", "cliff")));
        assertEquals("rock", classify(Map.of("natural", "rock")));
        assertEquals("rock", classify(Map.of("natural", "stone")));
        assertEquals("glacier", classify(Map.of("natural", "glacier")));
        assertEquals("bay", classify(Map.of("natural", "bay")));
        assertEquals("cape", classify(Map.of("natural", "cape")));
        assertEquals("beach", classify(Map.of("natural", "beach")));
        assertEquals("forest", classify(Map.of("natural", "wood")));
        assertEquals("forest", classify(Map.of("natural", "forest")));
    }

    @Test
    public void naturalSpringArmsClassified() {
        assertEquals("spring", classify(Map.of("natural", "spring")));
        assertEquals("spring", classify(Map.of("natural", "hot_spring")));
    }

    // --- waterway sub-types ---------------------------------------------------

    @Test
    public void waterwaySubtypesClassified() {
        assertEquals("waterfall", classify(Map.of("waterway", "waterfall")));
        assertEquals("river", classify(Map.of("waterway", "river")));
        assertEquals("stream", classify(Map.of("waterway", "stream")));
        assertEquals("canal", classify(Map.of("waterway", "canal")));
        assertEquals("rapids", classify(Map.of("waterway", "rapids")));
        // unknown waterway falls back to the generic "waterway" class
        assertEquals("waterway", classify(Map.of("waterway", "drain")));
    }

    // --- place sub-types ------------------------------------------------------

    @Test
    public void placeSubtypesClassified() {
        // city/town/village/hamlet/suburb/neighbourhood pass through as their own value
        assertEquals("city", classify(Map.of("place", "city")));
        assertEquals("hamlet", classify(Map.of("place", "hamlet")));
        assertEquals("suburb", classify(Map.of("place", "suburb")));
        assertEquals("neighbourhood", classify(Map.of("place", "neighbourhood")));
        assertEquals("island", classify(Map.of("place", "island")));
        assertEquals("island", classify(Map.of("place", "islet")));
        assertEquals("locality", classify(Map.of("place", "locality")));
        // unknown place falls back to the generic "place" class
        assertEquals("place", classify(Map.of("place", "county")));
    }

    // --- historic / tourism / leisure / craft ---------------------------------

    @Test
    public void historicAlwaysMapsToHistoric() {
        assertEquals("historic", classify(Map.of("historic", "castle")));
        assertEquals("historic", classify(Map.of("historic", "ruins")));
    }

    @Test
    public void tourismRemainingArmsClassified() {
        assertEquals("attraction", classify(Map.of("tourism", "attraction")));
        // information folds into the generic "tourism" class
        assertEquals("tourism", classify(Map.of("tourism", "information")));
        // unknown tourism falls back to "tourism"
        assertEquals("tourism", classify(Map.of("tourism", "zoo")));
    }

    @Test
    public void leisureUnknownFallsBackToLeisure() {
        assertEquals("leisure", classify(Map.of("leisure", "marina")));
    }

    @Test
    public void amenityTransitAndArtsArmsClassified() {
        assertEquals("transit", classify(Map.of("amenity", "bus_station")));
        assertEquals("museum", classify(Map.of("amenity", "theatre")));
    }

    @Test
    public void craftMapsToOffice() {
        // craft is the only primary key (so the else-if craft arm is exercised) -> "office".
        assertEquals("office", classify(Map.of("craft", "carpenter")));
    }

    // --- building guard + remaining building sub-types ------------------------

    @Test
    public void buildingCapitalNoIsNotClassified() {
        // The guard rejects "no"/"none"/"No"; capital-N "No" must also yield null, not "building".
        assertNull(classify(Map.of("building", "No")));
    }

    @Test
    public void buildingRemainingSubtypesClassified() {
        assertEquals("lodging", classify(Map.of("building", "hotel")));
        assertEquals("medical", classify(Map.of("building", "hospital")));
        assertEquals("education", classify(Map.of("building", "school")));
    }
}
