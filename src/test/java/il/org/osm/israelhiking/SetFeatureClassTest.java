package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Pins the OSM-tag to feature_class classifier across landform, water, place and built tags. */
@Tag("unit")
public class SetFeatureClassTest {

    private static Function<String, String> tags(Map<String, String> m) {
        return m::get;
    }

    private static String classify(Map<String, String> m) {
        return OsmTagUtils.classifyFeatureClass(tags(m));
    }

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

        assertEquals("river", classify(Map.of("natural", "water", "water", "river")));
        assertEquals("canal", classify(Map.of("natural", "water", "water", "canal")));
        assertEquals("lagoon", classify(Map.of("natural", "water", "water", "lagoon")));
        assertEquals("water", classify(Map.of("natural", "water", "water", "oxbow")));

        assertEquals("lake", classify(Map.of("natural", "water", "water", "lake")));
        assertEquals("reservoir", classify(Map.of("natural", "water", "water", "reservoir")));
        assertEquals("pond", classify(Map.of("natural", "water", "water", "pond")));
        assertEquals("lake", classify(Map.of("natural", "water")));
    }

    @Test
    public void unrecognisedNaturalFallsBackToNaturalCatchAll() {

        assertEquals("natural", classify(Map.of("natural", "sand")));
    }

    @Test
    public void railwayAndLanduseStayNull() {

        assertNull(classify(Map.of("railway", "station")));
        assertNull(classify(Map.of("landuse", "forest")));
    }

    @Test
    public void lodgingTourismMapsToLodging() {
        assertEquals("lodging", classify(Map.of("tourism", "hotel")));
        assertEquals("lodging", classify(Map.of("tourism", "guest_house")));
        assertEquals("museum", classify(Map.of("tourism", "museum")));

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

        assertEquals("building", classify(Map.of("building", "yes")));
        assertEquals("building", classify(Map.of("building", "hall")));

        assertNull(classify(Map.of("building", "no")));
        assertNull(classify(Map.of("building", "none")));
    }

    @Test
    public void outdoorWinsOverBuiltTags() {

        assertEquals("viewpoint", classify(Map.of("tourism", "viewpoint", "building", "yes")));
        assertEquals("peak", classify(Map.of("natural", "peak", "amenity", "restaurant")));
        assertEquals("lake", classify(Map.of("natural", "water", "leisure", "park")));
    }

    @Test
    public void naturalWinsOverOtherPrimaryTags() {

        assertEquals("canyon", classify(Map.of("natural", "canyon", "place", "locality")));
    }

    @Test
    public void noTagsYieldsNull() {
        assertNull(classify(Map.of("name", "Anonymous")));
    }

    @Test
    public void nullTagLookupResultYieldsNull() {

        assertNull(OsmTagUtils.classifyFeatureClass(t -> null));
    }

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

    @Test
    public void waterwaySubtypesClassified() {
        assertEquals("waterfall", classify(Map.of("waterway", "waterfall")));
        assertEquals("river", classify(Map.of("waterway", "river")));
        assertEquals("stream", classify(Map.of("waterway", "stream")));
        assertEquals("canal", classify(Map.of("waterway", "canal")));
        assertEquals("rapids", classify(Map.of("waterway", "rapids")));

        assertEquals("waterway", classify(Map.of("waterway", "drain")));
    }

    @Test
    public void placeSubtypesClassified() {

        assertEquals("city", classify(Map.of("place", "city")));
        assertEquals("hamlet", classify(Map.of("place", "hamlet")));
        assertEquals("suburb", classify(Map.of("place", "suburb")));
        assertEquals("neighbourhood", classify(Map.of("place", "neighbourhood")));
        assertEquals("island", classify(Map.of("place", "island")));
        assertEquals("island", classify(Map.of("place", "islet")));
        assertEquals("locality", classify(Map.of("place", "locality")));

        assertEquals("place", classify(Map.of("place", "county")));
    }

    @Test
    public void historicAlwaysMapsToHistoric() {
        assertEquals("historic", classify(Map.of("historic", "castle")));
        assertEquals("historic", classify(Map.of("historic", "ruins")));
    }

    @Test
    public void tourismRemainingArmsClassified() {
        assertEquals("attraction", classify(Map.of("tourism", "attraction")));

        assertEquals("tourism", classify(Map.of("tourism", "information")));

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

        assertEquals("office", classify(Map.of("craft", "carpenter")));
    }

    @Test
    public void buildingCapitalNoIsNotClassified() {

        assertNull(classify(Map.of("building", "No")));
    }

    @Test
    public void buildingRemainingSubtypesClassified() {
        assertEquals("lodging", classify(Map.of("building", "hotel")));
        assertEquals("medical", classify(Map.of("building", "hospital")));
        assertEquals("education", classify(Map.of("building", "school")));
    }
}
