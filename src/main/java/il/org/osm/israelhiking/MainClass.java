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
            var qrankPath = args.getString("qrank-path", "Path to qrank.csv.gz for the prominence signal (empty = run without it)", "");
            var qrankIndex = QRankIndex.load(qrankPath.isEmpty() ? null : Path.of(qrankPath));

            var targetPointsIndex = ElasticsearchHelper.createPointsIndex(esClient, pointsIndexAlias,
                    supportedLanguages);
            var targetBBoxIndex = ElasticsearchHelper.createBBoxIndex(esClient, bboxIndexAlias, supportedLanguages);
            var profile = new PlanetSearchProfile(planetiler.config(), esClient, targetPointsIndex, targetBBoxIndex,
                    supportedLanguages, qrankIndex);

            String area = args.getString("area", "geofabrik area to download", "israel-and-palestine");
            planetiler.setProfile(profile);
            // Geofabrik has no whole-planet file, so area=planet uses the aws:latest s3://osm-pds mirror.
            String osmSourceUrl = "planet".equals(area) ? "aws:latest" : "geofabrik:" + area;
            planetiler.addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), osmSourceUrl);
            if ("" != externalFilePath) {
                planetiler.addGeoJsonSource("external", Path.of(externalFilePath));
            }
            planetiler.overwriteOutput(Path.of("data", "target", PlanetSearchProfile.POINTS_LAYER_NAME + ".pmtiles"));
            planetiler.run();

            profile.finishIndexing();
            esClient.indices().refresh(r -> r.index(targetPointsIndex, targetBBoxIndex));

            ElasticsearchHelper.restoreSearchSettings(esClient, targetPointsIndex);
            ElasticsearchHelper.restoreSearchSettings(esClient, targetBBoxIndex);

            long emitted = profile.getEmittedCount();
            long indexed = profile.getIndexedCount();
            long failed = profile.getFailedCount();
            LOGGER.info("Indexing finished: indexed " + indexed + " of " + emitted
                    + " emitted document(s) (" + failed + " failed).");

            long abandoned = emitted - indexed - failed;
            if (abandoned != 0) {
                throw new IllegalStateException("Refusing alias swap. abandoned: " + abandoned);
            }

            boolean pointsSwapped = false;
            try {
                ElasticsearchHelper.switchAlias(esClient, pointsIndexAlias, targetPointsIndex);
                pointsSwapped = true;
                ElasticsearchHelper.switchAlias(esClient, bboxIndexAlias, targetBBoxIndex);
            } catch (Exception e) {
                LOGGER.severe("Alias swap failed. pointsSwapped: " + pointsSwapped + " error: " + e.getMessage());
                throw e;
            }
        } finally {
            esClient.close();
        }
    }
}
