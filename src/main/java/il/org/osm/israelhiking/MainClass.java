package il.org.osm.israelhiking;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import java.nio.file.Path;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.util.logging.Logger;
import java.util.logging.Level;

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
    
    private static String getTargetIndexName(String indexAlias, ElasticsearchClient esClient) throws Exception {
        var indexName = indexAlias + "1";
        if (!esClient.indices().existsAlias(c -> c.name(indexAlias)).value()) {
            Logger.getAnonymousLogger().info("Alias " + indexAlias + " does not exist, creating index " + indexName);
            return indexName;
        } 
        var alias = esClient.indices().getAlias(c -> c.name(indexAlias)).result();
        if (alias.containsKey(indexName)) {
            Logger.getAnonymousLogger().info("Alias " + indexAlias + " exists, creating index " + indexAlias + "2");
            return indexAlias + "2";
        }
        Logger.getAnonymousLogger().info("Alias " + indexAlias + " exists, creating index " + indexName);
        return indexName;
    }

    static void run(Arguments args) throws Exception {
        String area = args.getString("area", "geofabrik area to download", "israel-and-palestine");
        // Planetiler is a convenience wrapper around the lower-level API for the most common use-cases.
        // See ToiletsOverlayLowLevelApi for an example using the lower-level API
        Planetiler planetiler = Planetiler.create(args);
        
        Logger.getLogger("org.elasticsearch.client.RestClient").setLevel(Level.OFF);

        RestClient restClient = RestClient
            .builder(HttpHost.create(args.getString("es-address", "Elasticsearch address", "http://localhost:9200")))
            .build();

        ElasticsearchTransport transport = new RestClientTransport(
            restClient, new JacksonJsonpMapper());

        ElasticsearchClient esClient = new ElasticsearchClient(transport);

        var supportedLanguages = args.getString("languages", "Languages to support", "en,he");
        var indexAlias = args.getString("es-index-alias", "Elasticsearch index to populate", "points");
        
        var targetIndex = getTargetIndexName(indexAlias, esClient);
        if (esClient.indices().exists(c -> c.index(targetIndex)).value()) {
            esClient.indices().delete(c -> c.index(targetIndex));
        }
        esClient.indices().create(c -> c.index(targetIndex));

        var profile = new PlanetSearchProfile(planetiler.config(), esClient, targetIndex, supportedLanguages);        

        planetiler.setProfile(profile)
          // override this default with osm_path="path/to/data.osm.pbf"
          .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
          // override this default with mbtiles="path/to/output.mbtiles"
          .overwriteOutput(Path.of("data", PlanetSearchProfile.POINTS_LAYER_NAME + ".pmtiles"))
          .run();

        Logger.getAnonymousLogger().info("Creating alias " + indexAlias + " for index " + targetIndex);

        esClient.indices().updateAliases(c -> 
          c.actions(a -> a.remove(i -> i.index("*").alias(indexAlias)))
          .actions(a -> a.add(c2 -> c2.index(targetIndex).alias(indexAlias)))
        );

        esClient.close();
    }
}
