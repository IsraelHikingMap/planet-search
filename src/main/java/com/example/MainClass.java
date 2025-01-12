package com.example;

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
    
    static void run(Arguments args) throws Exception {
        String area = args.getString("area", "geofabrik area to download", "israel-and-palestine");
        // Planetiler is a convenience wrapper around the lower-level API for the most common use-cases.
        // See ToiletsOverlayLowLevelApi for an example using the lower-level API
        Planetiler planetiler = Planetiler.create(args);
        
        Logger.getLogger("org.elasticsearch.client.RestClient").setLevel(Level.OFF);

        RestClient restClient = RestClient
            .builder(HttpHost.create("http://localhost:9200"))
            .build();

        ElasticsearchTransport transport = new RestClientTransport(
            restClient, new JacksonJsonpMapper());

        ElasticsearchClient esClient = new ElasticsearchClient(transport);

        esClient.indices().delete(c -> c
            .index("points")
        );

        esClient.indices().create(c -> c
            .index("points")
        );


        var profile = new GlobalSearchProfile(planetiler.config(), esClient);

        planetiler.setProfile(profile)
          // override this default with osm_path="path/to/data.osm.pbf"
          .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
          // override this default with mbtiles="path/to/output.mbtiles"
          .overwriteOutput(Path.of("data", GlobalSearchProfile.POINTS_LAYER_NAME + ".pmtiles"))
          .run();

        esClient.close();
    }
}
