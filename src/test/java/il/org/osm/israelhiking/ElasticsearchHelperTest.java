package il.org.osm.israelhiking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.ExistsAliasRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.ObjectBuilder;

import java.util.function.Function;
import static org.mockito.Mockito.*;

@Tag("unit")
@ExtendWith(MockitoExtension.class)
class ElasticsearchServiceTest {
    @Mock
    private ElasticsearchClient esClient;
    @Mock
    private ElasticsearchIndicesClient indicesClient;
    private final String[] supportedLanguages = {"en", "es"};
    private final String indexAlias = "test-index";
    @BeforeEach
    void setUp() throws Exception {
        when(esClient.indices()).thenReturn(indicesClient);
    }
    @Test
    void testCreateElasticsearchClient() throws Exception {
        when(indicesClient.existsAlias(ArgumentMatchers.<Function<ExistsAliasRequest.Builder, ObjectBuilder<ExistsAliasRequest>>>any())).thenReturn(new BooleanResponse(false));
        when(indicesClient.exists(ArgumentMatchers.<Function<ExistsRequest.Builder, ObjectBuilder<ExistsRequest>>>any())).thenReturn(new BooleanResponse(true));

        ElasticsearchHelper.createPointsIndex(esClient, indexAlias, supportedLanguages);

        verify(indicesClient).delete(ArgumentMatchers.<Function<DeleteIndexRequest.Builder, ObjectBuilder<DeleteIndexRequest>>>any());
    }
}