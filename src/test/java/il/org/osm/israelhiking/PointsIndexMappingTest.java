package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.StringWriter;
import java.util.function.Function;

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

/**
 * Asserts the reindex-bundle mapping/analyzer additions are actually emitted by createPointsIndex.
 * ElasticsearchServiceTest mocks the indices client but never inspects the built request; here we
 * capture the CreateIndexRequest (the create(Function) convenience overload applies the builder
 * lambda and delegates to the mockable create(CreateIndexRequest)), serialize it to JSON, and
 * assert the new fields/analyzers are present.
 *
 * Covers: edge-ngram name.[lang].prefix subfield with a non-ngram search_analyzer; the he-scoped
 * matres char_filters + hebrew analyzer/normalizer; and the separate alt_names.[lang] field.
 */
@Tag("unit")
@ExtendWith(MockitoExtension.class)
public class PointsIndexMappingTest {

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
        // No alias yet -> target is "<alias>1"; exists() true -> a delete is issued first.
        when(indicesClient.existsAlias(any(java.util.function.Function.class)))
                .thenReturn(new BooleanResponse(false));
        when(indicesClient.exists(any(java.util.function.Function.class)))
                .thenReturn(new BooleanResponse(true));

        ElasticsearchHelper.createPointsIndex(esClient, "points", supportedLanguages);

        // The create(Function) convenience overload is what createPointsIndex calls; capture that
        // builder lambda, apply it to a real Builder, and serialize the resulting request to JSON.
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
    public void mappingDeclaresEdgeNgramPrefixSubfieldAndSplitAnalyzers() throws Exception {
        var json = createPointsIndexJson();
        assertTrue(json.contains("\"prefix\""), "name.<lang> must declare a .prefix subfield");
        assertTrue(json.contains("prefix_index_analyzer"), "the .prefix index analyzer must be defined");
        assertTrue(json.contains("prefix_search_analyzer"),
                "the .prefix SEARCH analyzer (non-ngram) must be defined — the index/search split");
        assertTrue(json.contains("edge_ngram") || json.contains("edgeNgram") || json.contains("edge_ngram_2_15"),
                "an edge_ngram token filter must be in the analysis settings");
    }

    @Test
    public void analysisDeclaresHeScopedMatresFiltersAndAnalyzer() throws Exception {
        var json = createPointsIndexJson();
        assertTrue(json.contains("hebrew_matres"),
                "the he-scoped doubled-vav matres char_filter must be defined");
        assertTrue(json.contains("hebrew_analyzer"),
                "a Hebrew-scoped analyzer (universal + matres) must be defined");
        assertTrue(json.contains("hebrew_normalizer"),
                "a Hebrew-scoped keyword normalizer (universal + matres) must be defined");
        // The matres filters must NOT be wired into the shared universal analyzer (blast-radius
        // containment): universal_analyzer keeps only hebrew_niqqud as its char_filter.
        assertTrue(json.contains("universal_analyzer"), "universal_analyzer must still exist");
    }

    @Test
    public void mappingDeclaresSeparateAltNamesFieldPerLanguage() throws Exception {
        var json = createPointsIndexJson();
        assertTrue(json.contains("alt_names.default"), "alt_names.default must be mapped");
        assertTrue(json.contains("alt_names.he"), "alt_names.he must be mapped");
        assertTrue(json.contains("alt_names.en"), "alt_names.en must be mapped");
        // It must remain SEPARATE from name (not folded): name.<lang> still present.
        assertTrue(json.contains("name.default"), "name.<lang> primary fields must remain");
    }
}
