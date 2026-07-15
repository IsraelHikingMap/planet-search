package il.org.osm.israelhiking;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main entry point
 */
public class MainClass {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainClass.class);

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
        var properties = new Properties();
        try (InputStream stream = MainClass.class.getResourceAsStream("/planet-search-version.properties")) {
            properties.load(stream);
        }
        var version = properties.getProperty("version");
        LOGGER.info("Starting planet-search {}", version);
        var pointsIndexAlias = args.getString("es-points-index-alias", "Elasticsearch index to populate points",
                "points");
        var bboxIndexAlias = args.getString("es-bbox-index-alias", "Elasticsearch index to populate bounding boxes",
                "bbox");
        var supportedLanguages = args.getString("languages", "Languages to support", "en,he,ru,ar,es").split(",");
        var esAddress = args.getString("es-address", "Elasticsearch address", "http://localhost:9200");
        boolean templatesOnly = args.getBoolean("update-templates-only",
                "Store the search templates of this build in Elasticsearch and exit, without building anything. "
                        + "Updates the queries of a live index without a reindex",
                false);
        if (templatesOnly) {
            var esClient = ElasticsearchHelper.createElasticsearchClient(esAddress);
            try {
                SearchTemplates.register(esClient, ElasticsearchHelper.allLanguages(supportedLanguages));
                LOGGER.info("Updated the search templates of {}, the indices were left as they are", esAddress);
            } finally {
                esClient.close();
            }
            return;
        }
        boolean skipTiles = args.getBoolean("skip-tiles",
                "Collapse tile output to z0 (near-instant archive) to speed up an ES-only reindex; "
                        + "map tiles become a stub. Default false — leave off when tiles are needed.",
                false);
        if (skipTiles) {
            LOGGER.warn("skip-tiles=true: collapsing tile output to z0. The ES search index is "
                    + "built normally; the .pmtiles archive will be a stub. Do NOT use for a build "
                    + "whose map tiles are consumed (client / production map).");
            // Override only the zoom args (not the whole Arguments) — do not re-broaden this.
            args = Arguments.of("maxzoom", "0", "render_maxzoom", "0").orElse(args);
        }
        Planetiler planetiler = Planetiler.create(args);

        var esClient = ElasticsearchHelper.createElasticsearchClient(esAddress);
        BulkIndexer bulkListener = null;
        try {
            var externalFilePath = args.getString("external-file-path", "External file path", "");
            var qrankPath = args.getString("qrank-path",
                    "Path to qrank.csv.gz for the prominence signal (empty = run without it)", "");
            var qrankLookup = QRankLookup.load(qrankPath.isBlank() ? null : Path.of(qrankPath));
            var context = ElasticsearchHelper.initRun(esClient, pointsIndexAlias, bboxIndexAlias,
                    supportedLanguages, qrankLookup);
            bulkListener = context.bulkListener();
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

            ElasticsearchHelper.finalizeRun(context);
        } finally {
            if (bulkListener != null) {
                bulkListener.close();
            }
            esClient.close();
        }
    }
}
