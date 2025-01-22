package il.org.osm.israelhiking;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class ElasticsearchHelper {
    public static ElasticsearchClient createElasticsearchClient(String esAddress) {
        Logger.getLogger("org.elasticsearch.client.RestClient").setLevel(Level.OFF);

        RestClient restClient = RestClient
            .builder(HttpHost.create(esAddress))
            .build();

        ElasticsearchTransport transport = new RestClientTransport(
            restClient, new JacksonJsonpMapper());

        return new ElasticsearchClient(transport);
    }

    public static String createPointsIndex(ElasticsearchClient esClient, String indexAlias, String[] supportedLanguages) throws Exception {
        var targetIndex = getTargetIndexName(indexAlias, esClient);
        if (esClient.indices().exists(c -> c.index(targetIndex)).value()) {
            esClient.indices().delete(c -> c.index(targetIndex));
        }
        esClient.indices().create(c -> 
            c.index(targetIndex)
            .mappings(m -> {
                for (var lang : supportedLanguages) {
                    m.properties("name." + lang, k -> k.text(p -> p.fields("keyword", f -> f.keyword(kw -> kw))));
                    m.properties("description." + lang, k -> k.text(p -> p.fields("keyword", f -> f.keyword(kw -> kw))));
                }
                m.properties("location", g -> g.geoPoint(p -> p));
                return m;
            })
        );

        return targetIndex;
    }

    public static String createBBoxIndex(ElasticsearchClient esClient, String indexAlias, String[] supportedLanguages) throws Exception {
        var targetIndex = getTargetIndexName(indexAlias, esClient);
        if (esClient.indices().exists(c -> c.index(targetIndex)).value()) {
            esClient.indices().delete(c -> c.index(targetIndex));
        }
        esClient.indices().create(c -> 
            c.index(targetIndex)
            .mappings(m -> {
                for (var lang : supportedLanguages) {
                    m.properties("name." + lang, k -> k.text(p -> p.fields("keyword", f -> f.keyword(kw -> kw))));
                }
                m.properties("bbox", g -> g.geoShape(p -> p));
                m.properties("area", n -> n.float_(f -> f));
                return m;
            })
        );

        return targetIndex;
    }

    private static String getTargetIndexName(String indexAlias, ElasticsearchClient esClient) throws Exception {
        var indexName = indexAlias + "1";
        if (!esClient.indices().existsAlias(c -> c.name(indexAlias)).value()) {
            return indexName;
        } 
        var alias = esClient.indices().getAlias(c -> c.name(indexAlias)).result();
        if (alias.containsKey(indexName)) {
            return indexAlias + "2";
        }
        return indexName;
    }

    public static void switchAlias(ElasticsearchClient esClient, String indexAlias, String targetIndex) throws Exception {
        esClient.indices().updateAliases(c -> 
          c.actions(a -> a.remove(i -> i.index("*").alias(indexAlias)))
          .actions(a -> a.add(c2 -> c2.index(targetIndex).alias(indexAlias)))
        );
    }
}
