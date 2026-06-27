package il.org.osm.israelhiking;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;

import co.elastic.clients.elasticsearch.inference.ElserServiceSettings;

import java.nio.file.Path;
import java.util.logging.Logger;

/**
 * The main entry point
 */
public class MainClass {

    private static final Logger LOGGER = Logger.getLogger(MainClass.class.getName());

    /** Static utility class should not be instantiated. */
    private MainClass() {
    }

    /**
     * Main entry point for the application.
     * 
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        run(Arguments.fromArgsOrConfigFile(args));
    }

    static void run(Arguments args) throws Exception {
        boolean skipTiles = args.getBoolean("skip-tiles",
                "Collapse tile output to z0 (near-instant archive) to speed up an ES-only reindex; "
                        + "map tiles become a stub. Default false — leave off when tiles are needed.",
                false);
        if (skipTiles) {
            LOGGER.warning("skip-tiles=true: collapsing tile output to z0. The ES search index is "
                    + "built normally; the .pmtiles archive will be a stub. Do NOT use for a build "
                    + "whose map tiles are consumed (client / production map).");
            // Override only the zoom args (not the whole Arguments) — do not re-broaden this.
            args = Arguments.of("maxzoom", "0", "render_maxzoom", "0").orElse(args);
        }
        Planetiler planetiler = Planetiler.create(args);

        var esAddress = args.getString("es-address", "Elasticsearch address", "http://localhost:9200");
        var esClient = ElasticsearchHelper.createElasticsearchClient(esAddress);
        try {
            var pointsIndexAlias = args.getString("es-points-index-alias", "Elasticsearch index to populate points",
                    "points");
            var bboxIndexAlias = args.getString("es-bbox-index-alias", "Elasticsearch index to populate bounding boxes",
                    "bbox");
            var supportedLanguages = args.getString("languages", "Languages to support", "en,he,ru,ar,es").split(",");
            var externalFilePath = args.getString("external-file-path", "External file path", "");
            var context = ElasticsearchHelper.initRun(esClient, pointsIndexAlias, bboxIndexAlias,
                    supportedLanguages);
            var profile = new PlanetSearchProfile(planetiler.config(), context);

            String area = args.getString("area", "geofabrik area to download", "israel-and-palestine");
            planetiler.setProfile(profile);
            // override this default with osm_path="path/to/data.osm.pbf"
            // Geofabrik has no whole-planet file, so area=planet uses the aws:latest
            // s3://osm-pds mirror.
            String osmSourceUrl = "planet".equals(area) ? "aws:latest" : "geofabrik:" + area;
            planetiler.addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), osmSourceUrl);
            if ("" != externalFilePath) {
                planetiler.addGeoJsonSource("external", Path.of(externalFilePath));
            }
            planetiler.overwriteOutput(Path.of("data", "target", PlanetSearchProfile.POINTS_LAYER_NAME + ".pmtiles"));
            planetiler.run();

            // Drain buffered inserts, refresh, restore search settings, and swap
            // the alias. This blocks on the BulkIngester so the failure counters
            // are final by the time the gate below reads them.
            ElasticsearchHelper.finalizeRun(context, profile);

            // bbox geo_shape rejects (a few degenerate OSM relation geometries ES
            // can't parse) are tolerated — they don't affect name search — but we
            // surface them so they're never silent.
            if (profile.getFailedBboxCount() > 0) {
                LOGGER.warning("Indexing dropped " + profile.getFailedBboxCount()
                        + " bbox document(s) (geo_shape rejects); tolerated — name search unaffected.");
            }

            // Fail the process if any POINTS document was dropped during indexing.
            // The BulkIngester listener already counts and logs failures; without a
            // non-zero exit here CI would mistake a partial points index for success
            // and silently ship missing search results. Exit non-zero so the build
            // fails loudly instead.
            if (profile.hasIndexingFailures()) {
                throw new IllegalStateException("Indexing finished with " + profile.getFailedPointsCount()
                        + " failed POINTS document(s) out of " + profile.getEmittedCount()
                        + " emitted (" + profile.getIndexedCount() + " indexed, "
                        + profile.getFailedBboxCount() + " bbox dropped). "
                        + "Refusing to treat a partial points index as success.");
            }
        } finally {
            esClient.close();
        }
    }
}
