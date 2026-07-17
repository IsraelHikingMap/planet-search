package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

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

    private static final Logger LOGGER = Logger.getLogger(E2ETest.class.getName());

    private static final String EXTERNAL_FILE = "./src/test/resources/external.geojson";
    private static final String POINTS_ALIAS = "points";
    private static final String BBOX_ALIAS = "bbox";

    private static final String ES_ADDRESS = "http://localhost:9200";

    // What this test runs on. The values below are the ones the CI runs, change
    // them here to score the queries against another part of the world, for
    // example North America, where most of the relevance cases are:
    //
    // AREA = "north-america" - an 18 GB extract, needs a bigger heap, i.e.
    // -DargLine="-Xmx24g", and takes a couple of hours
    // SEARCH_CASES = "/search-relevance-cases.json" - the 305 relevance cases
    // CONTAINER_CASES = "" - the container cases are Israeli, so skip them
    // USE_QRANK = true - prominence is only realistic with QRank, the file is
    // downloaded once into data/sources and reused
    private static final String AREA = "israel-and-palestine";
    private static final String SEARCH_CASES = "/search-sanity-cases.json";
    private static final String CONTAINER_CASES = "/search-container-cases.json";
    private static final boolean USE_QRANK = false;

    /** Downloaded on demand, next to the OSM extract planetiler downloads. */
    private static final Path QRANK_FILE = Path.of("data", "sources", "qrank.csv.gz");
    private static final String QRANK_URL = "https://qrank.toolforge.org/download/qrank.csv.gz";

    @Test
    public void test() throws Exception {
        var arguments = new ArrayList<String>(List.of("--download",
                "--area", AREA,
                "--external-file-path", EXTERNAL_FILE,
                "--es-address", ES_ADDRESS));
        if (USE_QRANK) {
            arguments.add("--qrank-path");
            arguments.add(downloadQrankIfMissing().toString());
        }

        MainClass.main(arguments.toArray(String[]::new));
        MainClass.main(arguments.toArray(String[]::new));

        try (var esClient = ElasticsearchHelper.createElasticsearchClient(ES_ADDRESS)) {
            assertEveryCaseIsFound(esClient, SEARCH_CASES);
            if (!CONTAINER_CASES.isBlank()) {
                assertEveryPointHasAContainer(esClient, CONTAINER_CASES);
            }
            assertPointsAreEnrichedWithContainers(esClient);
            assertPlaceFilterScopesToContainers(esClient);
            assertPlacesAreNotDuplicated(esClient);
        }
    }

    /**
     * A settlement that OSM maps as both a polygon and a place node must still be
     * a single searchable place: {@code processPlaceFeature} keeps the polygon as
     * a container only, so the node is not doubled by it. Each city below has both
     * representations in this extract, so running the real search query and
     * finding more than one place with the exact name means the dedup regressed.
     */
    private void assertPlacesAreNotDuplicated(ElasticsearchClient esClient) throws Exception {
        var cities = List.of("עפולה", "נס ציונה", "גן יבנה", "נצרת");
        var failures = new ArrayList<String>();
        for (var city : cities) {
            var exactPlaces = searchPoints(esClient, Map.of("searchTerm", JsonData.of(city))).stream()
                    .filter(point -> "icon-home".equals(point.poiIcon))
                    .filter(point -> point.name.containsValue(city))
                    .count();
            if (exactPlaces == 0) {
                failures.add("  " + city + ": the search returned no place with this exact name");
            } else if (exactPlaces > 1) {
                failures.add("  " + city + ": the search returned " + exactPlaces
                        + " places with this exact name (a same-named polygon was indexed as a second point)");
            }
        }
        if (!failures.isEmpty()) {
            fail("places were duplicated or missing:\n" + String.join("\n", failures));
        }
    }

    /**
     * The second build tags every point with the places that contain it. Points
     * for cities deep inside the country must come back with their containers,
     * which proves the enrichment ran and did not break indexing.
     */
    private void assertPointsAreEnrichedWithContainers(ElasticsearchClient esClient) throws Exception {
        var terms = List.of("חיפה", "תל אביב", "ירושלים");
        var failures = new ArrayList<String>();
        for (var term : terms) {
            var point = topPoint(esClient, term);
            if (point == null) {
                failures.add("  " + term + ": no hit at all");
            } else if (point.poiParentNames == null || point.poiParentNames.isEmpty()) {
                failures.add("  " + term + ": no containers attached (parentNames=" + point.poiParentNames
                        + ", container=" + point.poiContainer + ", country=" + point.poiCountry + ")");
            }
        }
        if (!failures.isEmpty()) {
            fail(failures.size() + " of " + terms.size() + " points were not enriched with containers:\n"
                    + String.join("\n", failures));
        }
    }

    /**
     * The {@code place} parameter scopes a search to points whose containers
     * include that place — the single query behind a "point, place" search. A
     * point must survive a filter on one of its own containers and be dropped by
     * a filter on a place that contains nothing.
     */
    private void assertPlaceFilterScopesToContainers(ElasticsearchClient esClient) throws Exception {
        var term = "חיפה";
        var unscoped = searchPoints(esClient, Map.of("searchTerm", JsonData.of(term)));
        if (unscoped.isEmpty() || unscoped.get(0).poiParentNames == null
                || unscoped.get(0).poiParentNames.isEmpty()) {
            fail("cannot check the place filter: \"" + term + "\" returned no enriched point");
        }
        var container = unscoped.get(0).poiParentNames.values().stream()
                .flatMap(List::stream).findFirst().orElseThrow();

        var scopedToContainer = searchPoints(esClient,
                Map.of("searchTerm", JsonData.of(term), "place", JsonData.of(container)));
        var scopedToNowhere = searchPoints(esClient,
                Map.of("searchTerm", JsonData.of(term), "place", JsonData.of("לא-מקום-שקיים")));

        var failures = new ArrayList<String>();
        if (scopedToContainer.isEmpty()) {
            failures.add("  place=\"" + container + "\" (a real container of " + term + ") returned nothing");
        }
        if (!scopedToNowhere.isEmpty()) {
            failures.add("  place=\"לא-מקום-שקיים\" (contains nothing) still returned "
                    + scopedToNowhere.size() + " hit(s)");
        }
        if (!failures.isEmpty()) {
            fail("the place filter did not scope the search:\n" + String.join("\n", failures));
        }
    }

    private PointDocument topPoint(ElasticsearchClient esClient, String term) throws Exception {
        var hits = searchPoints(esClient, Map.of("searchTerm", JsonData.of(term)));
        return hits.isEmpty() ? null : hits.get(0);
    }

    private List<PointDocument> searchPoints(ElasticsearchClient esClient, Map<String, JsonData> params)
            throws Exception {
        var response = esClient.searchTemplate(s -> s
                .index(POINTS_ALIAS)
                .id(SearchTemplates.POINTS_SEARCH)
                .params(params),
                PointDocument.class);
        return response.hits().hits().stream().map(hit -> hit.source()).toList();
    }

    /**
     * The prominence of a point depends on QRank, so scoring the queries against
     * an index that was built without it is scoring against a different world.
     * The file is ~360 MB, so it is downloaded once, next to the OSM extract, and
     * reused by every later run.
     */
    private Path downloadQrankIfMissing() throws Exception {
        if (Files.exists(QRANK_FILE) && Files.size(QRANK_FILE) > 0) {
            LOGGER.info("Using the QRank file that is already there: " + QRANK_FILE);
            return QRANK_FILE;
        }
        LOGGER.info("Downloading QRank from " + QRANK_URL + ", this takes a while");
        Files.createDirectories(QRANK_FILE.getParent());
        var partial = QRANK_FILE.resolveSibling("qrank.csv.gz.partial");
        try (var http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build()) {
            var request = HttpRequest.newBuilder(URI.create(QRANK_URL)).GET().build();
            var response = http.send(request, HttpResponse.BodyHandlers.ofFile(partial));
            if (response.statusCode() != 200) {
                Files.deleteIfExists(partial);
                throw new IllegalStateException("Failed to download QRank, got HTTP " + response.statusCode());
            }
        }
        // Only name it once it is whole, so that a broken download is not reused.
        Files.move(partial, QRANK_FILE, StandardCopyOption.REPLACE_EXISTING);
        LOGGER.info("Downloaded QRank to " + QRANK_FILE + " (" + Files.size(QRANK_FILE) / 1024 / 1024 + " MB)");
        return QRANK_FILE;
    }

    /**
     * A point should find the place that contains it, also when the polygon of
     * that place is a self intersecting one, which Elasticsearch refuses to
     * index as is.
     */
    private void assertEveryPointHasAContainer(ElasticsearchClient esClient, String casesResource) throws Exception {
        var mapper = new ObjectMapper();
        List<ContainerCase> cases = mapper.readValue(
                getClass().getResourceAsStream(casesResource),
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
    private void assertEveryCaseIsFound(ElasticsearchClient esClient, String casesResource) throws Exception {
        var cases = SearchCases.load(casesResource);
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
