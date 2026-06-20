package il.org.osm.israelhiking;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;

import co.elastic.clients.elasticsearch.inference.ElserServiceSettings;

import java.nio.file.Path;

/**
 * The main entry point
 */
public class MainClass {

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

            boolean finalized = false;
            try {
                planetiler.run();
                ElasticsearchHelper.finalizeRun(context);
                finalized = true;
            } finally {
                if (!finalized) {
                    ElasticsearchHelper.abortRun(context);
                }
            }
        } finally {
            esClient.close();
        }
    }
}
