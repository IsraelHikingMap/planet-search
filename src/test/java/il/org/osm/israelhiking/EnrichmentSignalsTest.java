package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;

import il.org.osm.israelhiking.ElasticsearchHelper.ElasticRunContext;

/** Pins convertTagsToDocument's enrichment wiring and setEnrichmentSignals' area, catch and intermittent branches. */
@Tag("unit")
public class EnrichmentSignalsTest {

    private static final String[] LANGS = { "en", "he", "ru", "ar", "es" };
    private static final GeometryFactory GF = new GeometryFactory();

    private static PlanetSearchProfile profile() throws Exception {
        var context = new ElasticRunContext(null, "points", "bbox", "points1", "bbox1", LANGS);
        var config = PlanetilerConfig.defaults();
        for (var ctor : PlanetSearchProfile.class.getDeclaredConstructors()) {
            var params = ctor.getParameterTypes();
            if (params.length == 2) {
                return (PlanetSearchProfile) ctor.newInstance(config, context);
            }
            if (params.length == 3) {
                return (PlanetSearchProfile) ctor.newInstance(config, context, emptyOf(params[2]));
            }
        }
        throw new IllegalStateException("no usable PlanetSearchProfile constructor");
    }

    private static Object emptyOf(Class<?> qrankIndexType) throws Exception {
        var empty = qrankIndexType.getDeclaredMethod("empty");
        empty.setAccessible(true);
        return empty.invoke(null);
    }

    private static void convert(PlanetSearchProfile p, PointDocument doc, WithTags feature) throws Exception {
        Method m = PlanetSearchProfile.class.getDeclaredMethod(
                "convertTagsToDocument", PointDocument.class, WithTags.class);
        m.setAccessible(true);
        m.invoke(p, doc, feature);
    }

    private static void enrich(PlanetSearchProfile p, PointDocument doc, WithTags feature) throws Exception {
        Method m = PlanetSearchProfile.class.getDeclaredMethod(
                "setEnrichmentSignals", PointDocument.class, WithTags.class);
        m.setAccessible(true);
        m.invoke(p, doc, feature);
    }

    private static Polygon square(double side) {
        Coordinate[] ring = {
            new Coordinate(0, 0), new Coordinate(0, side),
            new Coordinate(side, side), new Coordinate(side, 0), new Coordinate(0, 0)
        };
        return GF.createPolygon(ring);
    }

    private static SourceFeature latLonFeature(Geometry geom, Map<String, Object> tags, long id) {
        return SimpleFeature.create(geom, tags, "test", "test", id);
    }

    @Test
    public void convertWritesEnrichmentSignalsForPolygonFeature() throws Exception {
        var feature = latLonFeature(square(0.01),
                Map.of("name", "Lake Test", "natural", "water", "water", "lake",
                        "place", "village", "population", "2000",
                        "alt_name", "Test Lake", "intermittent", "yes"),
                1L);
        var doc = new PointDocument();
        convert(profile(), doc, feature);

        assertEquals("lake", doc.poiFeatureClass);
        assertEquals(List.of("Test Lake"), doc.alt_names.get("default"));
        assertEquals(2000, doc.population);
        assertTrue(doc.poiAreaNormalized != null && doc.poiAreaNormalized > 0f,
                "a buildable polygon must get a positive normalized area");
        assertEquals(Boolean.TRUE, doc.intermittent);
    }

    @Test
    public void nonPolygonPointLeavesAreaAndIntermittentUnset() throws Exception {
        var point = latLonFeature(GF.createPoint(new Coordinate(0, 0)),
                Map.of("name", "A Node"), 2L);
        var doc = new PointDocument();
        enrich(profile(), doc, point);

        assertNull(doc.poiAreaNormalized, "a point has no polygon area");
        assertNull(doc.intermittent);
    }

    @Test
    public void intermittentTagSetsTheFlagWithoutAnArea() throws Exception {
        var point = latLonFeature(GF.createPoint(new Coordinate(0, 0)),
                Map.of("waterway", "stream", "intermittent", "yes"), 3L);
        var doc = new PointDocument();
        enrich(profile(), doc, point);

        assertEquals(Boolean.TRUE, doc.intermittent);
        assertNull(doc.poiAreaNormalized);
    }

    @Test
    public void nonSourceFeatureWithTagsSkipsAreaAndStillReadsIntermittent() throws Exception {
        WithTags feature = WithTags.from(Map.of("intermittent", "yes"));
        var doc = new PointDocument();
        enrich(profile(), doc, feature);

        assertNull(doc.poiAreaNormalized, "a non-SourceFeature has no geometry to area-normalize");
        assertEquals(Boolean.TRUE, doc.intermittent);
    }

    @Test
    public void unbuildablePolygonAreaIsSkippedNotThrown() throws Exception {
        Logger logger = Logger.getLogger(PlanetSearchProfile.class.getName());
        Level previous = logger.getLevel();
        logger.setLevel(Level.FINE);
        try {
            var feature = new ThrowingPolygonFeature(Map.of("name", "Broken Area"), 11L);
            var doc = new PointDocument();
            enrich(profile(), doc, feature);
            assertNull(doc.poiAreaNormalized, "an unbuildable polygon must leave the area unset, not throw");
        } finally {
            logger.setLevel(previous);
        }
    }

    private static final class ThrowingPolygonFeature extends SourceFeature {
        ThrowingPolygonFeature(Map<String, Object> tags, long id) {
            super(tags, "test", "test", null, id);
        }

        @Override
        public Geometry worldGeometry() throws GeometryException {
            throw new GeometryException("test", "unbuildable polygon");
        }

        @Override
        public Geometry latLonGeometry() throws GeometryException {
            throw new GeometryException("test", "unbuildable polygon");
        }

        @Override
        public boolean isPoint() {
            return false;
        }

        @Override
        public boolean canBePolygon() {
            return true;
        }

        @Override
        public boolean canBeLine() {
            return false;
        }
    }
}
