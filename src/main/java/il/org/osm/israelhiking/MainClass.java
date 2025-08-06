package il.org.osm.israelhiking;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;

import java.nio.file.Path;

/**
 * The main entry point
 */
public class MainClass {

    /** Static utility class should not be instantiated. */
    private MainClass() {}

    /** 
     * Main entry point for the application.
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        run(Arguments.fromArgsOrConfigFile(args));
    }

    static void run(Arguments args) throws Exception {
        // Planetiler is a convenience wrapper around the lower-level API for the most common use-cases.
        // See ToiletsOverlayLowLevelApi for an example using the lower-level API
        Planetiler planetiler = Planetiler.create(args);
        
        var esAddress = args.getString("es-address", "Elasticsearch address", "http://localhost:9200");
        var esClient = ElasticsearchHelper.createElasticsearchClient(esAddress);
        try {
            var pointsIndexAlias = args.getString("es-points-index-alias", "Elasticsearch index to populate points", "points");
            var bboxIndexAlias = args.getString("es-bbox-index-alias", "Elasticsearch index to populate bounding boxes", "bbox");
            var supportedLanguages = args.getString("languages", "Languages to support", "en,he,ru,ar").split(",");
            var externalFilePath = args.getString("external-file-path", "External file path", "");
            var targetPointsIndex = ElasticsearchHelper.createPointsIndex(esClient, pointsIndexAlias, supportedLanguages);
            var targetBBoxIndex = ElasticsearchHelper.createBBoxIndex(esClient, bboxIndexAlias, supportedLanguages);
            var profile = new PlanetSearchProfile(planetiler.config(), esClient, targetPointsIndex, targetBBoxIndex, supportedLanguages);

            String area = args.getString("area", "geofabrik area to download", "israel-and-palestine");
            planetiler.setProfile(profile);
            // override this default with osm_path="path/to/data.osm.pbf"
            planetiler.addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area);
            if ("" != externalFilePath) {
                planetiler.addGeoJsonSource("external", Path.of(externalFilePath));
            }
            // override this default with mbtiles="path/to/output.mbtiles"
            planetiler.overwriteOutput(Path.of("data", "target", PlanetSearchProfile.POINTS_LAYER_NAME + ".pmtiles"));
            planetiler.run();

            ElasticsearchHelper.switchAlias(esClient, pointsIndexAlias, targetPointsIndex);
            ElasticsearchHelper.switchAlias(esClient, bboxIndexAlias, targetBBoxIndex);
        } finally {
            esClient.close();
        }
    }
}
