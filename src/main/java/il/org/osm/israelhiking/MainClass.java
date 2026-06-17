package il.org.osm.israelhiking;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;

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
        // skip-tiles (default OFF): collapse the vector-tile pyramid to a trivial z0 tile so the
        // .pmtiles archive write becomes near-instant for an ES-only reindex. ES ingest is
        // independent of tile zoom, so the search index is built identically; only map tiles are
        // degraded to a stub. Leave OFF for any build whose tiles are consumed.
        boolean skipTiles = args.getBoolean("skip-tiles",
                "Collapse tile output to z0 (near-instant archive) to speed up an ES-only reindex; "
                        + "map tiles become a stub. Default false — leave off when tiles are needed.",
                false);
        if (skipTiles) {
            LOGGER.warning("skip-tiles=true: collapsing tile output to z0 (maxzoom=0). The ES search "
                    + "index is built normally; the .pmtiles archive will be a stub. Do NOT use for a "
                    + "build whose map tiles are consumed (client / production map).");
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
            var qrankPath = args.getString("qrank-path",
                    "Path to qrank.csv.gz for the prominence signal (empty = run without it)", "");
            var qrankIndex = QRankIndex.load(qrankPath.isEmpty() ? null : Path.of(qrankPath));

            var targetPointsIndex = ElasticsearchHelper.createPointsIndex(esClient, pointsIndexAlias,
                    supportedLanguages);
            var targetBBoxIndex = ElasticsearchHelper.createBBoxIndex(esClient, bboxIndexAlias, supportedLanguages);
            var profile = new PlanetSearchProfile(planetiler.config(), esClient, targetPointsIndex, targetBBoxIndex,
                    supportedLanguages, qrankIndex);

            String area = args.getString("area", "geofabrik area to download", "israel-and-palestine");
            planetiler.setProfile(profile);
            // Geofabrik has no whole-planet file, so area=planet uses the aws:latest source (the
            // s3://osm-pds mirror with the full planet.osm.pbf); everything else uses geofabrik:<area>.
            // override this default with osm_path="path/to/data.osm.pbf"
            String osmSourceUrl = "planet".equals(area) ? "aws:latest" : "geofabrik:" + area;
            planetiler.addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), osmSourceUrl);
            if ("" != externalFilePath) {
                planetiler.addGeoJsonSource("external", Path.of(externalFilePath));
            }
            // override this default with mbtiles="path/to/output.mbtiles"
            planetiler.overwriteOutput(Path.of("data", "target", PlanetSearchProfile.POINTS_LAYER_NAME + ".pmtiles"));
            planetiler.run();

            // Flush, drain retries, and wait for in-flight bulk requests before the alias swap, then
            // refresh so docs are searchable.
            profile.finishIndexing();
            esClient.indices().refresh(r -> r.index(targetPointsIndex, targetBBoxIndex));

            // Restore normal refresh/replica settings (disabled during the build) before going live.
            ElasticsearchHelper.restoreSearchSettings(esClient, targetPointsIndex);
            ElasticsearchHelper.restoreSearchSettings(esClient, targetBBoxIndex);

            ElasticsearchHelper.switchAlias(esClient, pointsIndexAlias, targetPointsIndex);
            ElasticsearchHelper.switchAlias(esClient, bboxIndexAlias, targetBBoxIndex);

            LOGGER.info("Indexing finished: indexed " + profile.getIndexedCount() + " of "
                    + profile.getEmittedCount() + " emitted document(s) (" + profile.getFailedCount()
                    + " failed).");
        } finally {
            esClient.close();
        }
    }
}
