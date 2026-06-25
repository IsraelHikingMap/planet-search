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

/** Asserts createPointsIndex maps the computed ranking-signal fields. */
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
    public void mappingDeclaresComputedRankingFields() throws Exception {
        var json = createPointsIndexJson();
        assertTrue(json.contains("poiProminence"), "poiProminence must be mapped");
        assertTrue(json.contains("population"), "population must be mapped");
        assertTrue(json.contains("poiFeatureClass"), "poiFeatureClass must be mapped");
        assertTrue(json.contains("poiAreaNormalized"), "poiAreaNormalized must be mapped");
        assertTrue(json.contains("intermittent"), "intermittent must be mapped");

        assertTrue(json.contains("name.default"), "name.<lang> primary fields must remain");
    }
}
