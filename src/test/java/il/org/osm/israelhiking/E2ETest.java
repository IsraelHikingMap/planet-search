package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.JsonData;

import il.org.osm.israelhiking.SearchCases.Case;
import il.org.osm.israelhiking.SearchCases.Hit;

/**
 * Builds a real index out of a real OSM extract, and then searches it with the
 * search templates, to make sure that what the profile indexed can actually be
 * found by the queries the query side runs.
 */
@Tag("e2e")
public class E2ETest {

    private static final String EXTERNAL_FILE = "./src/test/resources/external.geojson";
    private static final String POINTS_ALIAS = "points";
    private static final String BBOX_ALIAS = "bbox";

    @Test
    public void test() throws Exception {
        var esAddress = System.getProperty("es.address", "http://localhost:9200");

        MainClass.main(new String[] { "--download", "--external-file-path", EXTERNAL_FILE,
                "--es-address", esAddress });

        try (var esClient = ElasticsearchHelper.createElasticsearchClient(esAddress)) {
            assertEveryCaseIsFound(esClient);
            assertEveryPointHasAContainer(esClient);
        }
    }

    /**
     * A point should find the place that contains it, also when the polygon of
     * that place is a self intersecting one, which Elasticsearch refuses to
     * index as is.
     */
    private void assertEveryPointHasAContainer(ElasticsearchClient esClient) throws Exception {
        var mapper = new ObjectMapper();
        List<ContainerCase> cases = mapper.readValue(
                getClass().getResourceAsStream("/search-container-cases.json"),
                mapper.getTypeFactory().constructCollectionType(List.class, ContainerCase.class));
        var failures = new ArrayList<String>();
        for (var containerCase : cases) {
            var response = esClient.searchTemplate(s -> s
                    .index(BBOX_ALIAS)
                    .id(SearchTemplates.BBOX_CONTAINS)
                    .params("shape", JsonData.of(Map.of(
                            "type", "Point",
                            "coordinates", List.of(containerCase.lng(), containerCase.lat())))),
                    JsonNode.class);
            var containers = response.hits().hits().stream()
                    .map(hit -> hit.source().path("name").path("he").asText())
                    .toList();
            if (!containers.contains(containerCase.expectedContainer())) {
                failures.add(String.format("  %s: expected the container \"%s\", got %s",
                        containerCase.id(), containerCase.expectedContainer(),
                        containers.isEmpty() ? "no container at all" : containers));
            }
        }
        if (!failures.isEmpty()) {
            fail(failures.size() + " of " + cases.size() + " container cases failed:\n"
                    + String.join("\n", failures));
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ContainerCase(String id, double lat, double lng, String expectedContainer) {
    }

    /**
     * Searches the index that was just built, and fails with all the cases that
     * did not find what they were looking for, not only the first one.
     */
    private void assertEveryCaseIsFound(ElasticsearchClient esClient) throws Exception {
        var cases = SearchCases.load("/search-sanity-cases.json");
        var failures = new ArrayList<String>();
        for (var searchCase : cases) {
            var failure = SearchCases.failure(searchCase, search(esClient, searchCase));
            if (failure != null) {
                failures.add("  " + failure);
            }
        }
        if (!failures.isEmpty()) {
            fail(failures.size() + " of " + cases.size() + " search cases failed:\n"
                    + String.join("\n", failures));
        }
    }

    private List<Hit> search(ElasticsearchClient esClient, Case searchCase) throws Exception {
        var parameters = new HashMap<String, JsonData>();
        parameters.put("searchTerm", JsonData.of(searchCase.searchTerm()));
        if (searchCase.isPrefix()) {
            parameters.put("prefix", JsonData.of(true));
        }
        if (searchCase.hasCenter()) {
            parameters.put("hasCenter", JsonData.of(true));
            parameters.put("lat", JsonData.of(searchCase.center().get(0)));
            parameters.put("lng", JsonData.of(searchCase.center().get(1)));
            parameters.put("zoom", JsonData.of(searchCase.zoom() == null ? 12 : searchCase.zoom()));
        }
        var templateId = searchCase.template() == null ? SearchTemplates.POINTS_SEARCH : searchCase.template();
        var response = esClient.searchTemplate(s -> s
                .index(POINTS_ALIAS)
                .id(templateId)
                .params(parameters), PointDocument.class);
        return response.hits().hits().stream()
                .map(hit -> hit.source())
                .map(document -> new Hit(
                        document.name.getOrDefault(searchCase.uiLanguage(), document.name.get("default")),
                        document.location[1],
                        document.location[0]))
                .toList();
    }
}
