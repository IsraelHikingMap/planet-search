package il.org.osm.israelhiking;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;

import java.nio.file.Path;

/**
 * The main entry point
 */
public class MainClass {
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
            var supportedLanguages = args.getString("languages", "Languages to support", "en,he").split(",");
            var externalFilePath = Path.of(args.getString("external-file-path", "Extranl file path", "data/sources/external.geojson"));
            var targetPointsIndex = ElasticsearchHelper.createPointsIndex(esClient, pointsIndexAlias, supportedLanguages);
            var targetBBoxIndex = ElasticsearchHelper.createBBoxIndex(esClient, bboxIndexAlias, supportedLanguages);
            var profile = new PlanetSearchProfile(planetiler.config(), esClient, targetPointsIndex, targetBBoxIndex, supportedLanguages);

            String area = args.getString("area", "geofabrik area to download", "israel-and-palestine");
            planetiler.setProfile(profile)
            // override this default with osm_path="path/to/data.osm.pbf"
            .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
            .addGeoJsonSource("external", externalFilePath)
            // override this default with mbtiles="path/to/output.mbtiles"
            .overwriteOutput(Path.of("data", "target", PlanetSearchProfile.POINTS_LAYER_NAME + ".pmtiles"))
            .run();

            ElasticsearchHelper.switchAlias(esClient, pointsIndexAlias, targetPointsIndex);
            ElasticsearchHelper.switchAlias(esClient, bboxIndexAlias, targetBBoxIndex);
        } finally {
            esClient.close();
        }
    }
}
