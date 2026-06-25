package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.StringWriter;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.ObjectBuilder;
import jakarta.json.stream.JsonGenerator;

/** Asserts createPointsIndex emits the prefix and Hebrew-matres analyzers and the name.prefix subfield. */
@Tag("unit")
@ExtendWith(MockitoExtension.class)
public class PointsAnalyzerMappingTest {

    @Mock
    private ElasticsearchClient esClient;

    @Mock
    private ElasticsearchIndicesClient indicesClient;

    private final String[] supportedLanguages = { "en", "he", "ru", "ar", "es" };

    @BeforeEach
    void setUp() {
        when(esClient.indices()).thenReturn(indicesClient);
    }

    private String createPointsIndexJson() throws Exception {
        when(indicesClient.existsAlias(any(java.util.function.Function.class)))
                .thenReturn(new BooleanResponse(false));
        when(indicesClient.exists(any(java.util.function.Function.class)))
                .thenReturn(new BooleanResponse(true));

        ElasticsearchHelper.createPointsIndex(esClient, "points", supportedLanguages);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Function<CreateIndexRequest.Builder, ObjectBuilder<CreateIndexRequest>>> captor =
                ArgumentCaptor.forClass(Function.class);
        verify(indicesClient).create(captor.capture());

        CreateIndexRequest request = captor.getValue()
                .apply(new CreateIndexRequest.Builder())
                .build();

        JsonpMapper mapper = new JacksonJsonpMapper();
        var sw = new StringWriter();
        try (JsonGenerator gen = mapper.jsonProvider().createGenerator(sw)) {
            request.serialize(gen, mapper);
        }
        return sw.toString();
    }

    @Test
    public void declaresEdgeNgramPrefixSubfieldWithSplitAnalyzers() throws Exception {
        var json = createPointsIndexJson();
        assertTrue(json.contains("\"prefix\""), "name.<lang> must declare a .prefix subfield");
        assertTrue(json.contains("prefix_index_analyzer"), "the .prefix index analyzer must be defined");
        assertTrue(json.contains("prefix_search_analyzer"),
                "the .prefix SEARCH analyzer (non-ngram) must be defined — the index/search split");
        assertTrue(json.contains("edge_ngram_2_15"), "an edge_ngram token filter must be in the settings");
    }

    @Test
    public void declaresHebrewMatresFiltersAndAnalyzers() throws Exception {
        var json = createPointsIndexJson();
        assertTrue(json.contains("hebrew_matres"), "the doubled-vav matres char_filter must be defined");
        assertTrue(json.contains("hebrew_analyzer"), "a Hebrew-scoped analyzer must be defined");
        assertTrue(json.contains("hebrew_normalizer"), "a Hebrew-scoped keyword normalizer must be defined");
        assertTrue(json.contains("hebrew_prefix_index_analyzer"),
                "the he-scoped prefix INDEX analyzer (matres + edge_ngram) must be defined");
        assertTrue(json.contains("hebrew_prefix_search_analyzer"),
                "the he-scoped prefix SEARCH analyzer (matres, no edge_ngram) must be defined");

        assertTrue(json.contains("universal_analyzer"), "universal_analyzer must still exist");
    }

    @Test
    public void altNamesHebrewCarriesHebrewAnalyzerAndPrefix() throws Exception {
        var json = createPointsIndexJson();
        JsonNode props = new ObjectMapper().readTree(json)
                .path("mappings").path("properties");
        JsonNode altHe = props.path("alt_names.he");
        assertEquals("hebrew_analyzer", altHe.path("analyzer").asText(),
                "alt_names.he must use the Hebrew analyzer, mirroring name.he");
        assertEquals("hebrew_normalizer",
                altHe.path("fields").path("keyword").path("normalizer").asText(),
                "alt_names.he.keyword must use the Hebrew normalizer");
        assertEquals("hebrew_prefix_index_analyzer",
                altHe.path("fields").path("prefix").path("analyzer").asText(),
                "alt_names.he must expose the he-scoped prefix subfield");
        assertEquals("universal_analyzer", props.path("alt_names.en").path("analyzer").asText(),
                "alt_names.en stays on the universal analyzer");
    }
}
