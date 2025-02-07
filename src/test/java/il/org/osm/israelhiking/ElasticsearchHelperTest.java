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
import co.elastic.clients.elasticsearch.indices.GetAliasRequest;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.get_alias.IndexAliases;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.ObjectBuilder;

import java.util.HashMap;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void testCreatePointIndexThatDoesntHaveAlias_ShouldDeleteItBeforeCreating() throws Exception {
        when(indicesClient.existsAlias(ArgumentMatchers.<Function<ExistsAliasRequest.Builder, ObjectBuilder<ExistsAliasRequest>>>any())).thenReturn(new BooleanResponse(false));
        when(indicesClient.exists(ArgumentMatchers.<Function<ExistsRequest.Builder, ObjectBuilder<ExistsRequest>>>any())).thenReturn(new BooleanResponse(true));

        var indexName = ElasticsearchHelper.createPointsIndex(esClient, indexAlias, supportedLanguages);

        verify(indicesClient).delete(ArgumentMatchers.<Function<DeleteIndexRequest.Builder, ObjectBuilder<DeleteIndexRequest>>>any());
        assertEquals(indexAlias + "1", indexName);
    }

    @Test
    void testCreatePointIndexWhenOneExists_ShouldAddASecondOne() throws Exception {
        when(indicesClient.existsAlias(ArgumentMatchers.<Function<ExistsAliasRequest.Builder, ObjectBuilder<ExistsAliasRequest>>>any())).thenReturn(new BooleanResponse(true));
        when(indicesClient.exists(ArgumentMatchers.<Function<ExistsRequest.Builder, ObjectBuilder<ExistsRequest>>>any())).thenReturn(new BooleanResponse(true));
        when(indicesClient.getAlias(ArgumentMatchers.<Function<GetAliasRequest.Builder, ObjectBuilder<GetAliasRequest>>>any()))
            .thenReturn(GetAliasResponse.of(b -> b.result(new HashMap<String, IndexAliases>() { 
                {
                    put(indexAlias + "1", null);
                }
            })));

        var indexName = ElasticsearchHelper.createPointsIndex(esClient, indexAlias, supportedLanguages);

        assertEquals(indexAlias + "2", indexName);
    }

    @Test
    void testCreatePointIndexWhenSecondaryExists_ShouldAddTheFirstAndDelete() throws Exception {
        when(indicesClient.existsAlias(ArgumentMatchers.<Function<ExistsAliasRequest.Builder, ObjectBuilder<ExistsAliasRequest>>>any())).thenReturn(new BooleanResponse(true));
        when(indicesClient.exists(ArgumentMatchers.<Function<ExistsRequest.Builder, ObjectBuilder<ExistsRequest>>>any())).thenReturn(new BooleanResponse(true));
        when(indicesClient.getAlias(ArgumentMatchers.<Function<GetAliasRequest.Builder, ObjectBuilder<GetAliasRequest>>>any()))
            .thenReturn(GetAliasResponse.of(b -> b.result(new HashMap<String, IndexAliases>() { 
                {
                    put(indexAlias + "2", null);
                }
            })));

        var indexName = ElasticsearchHelper.createPointsIndex(esClient, indexAlias, supportedLanguages);

        assertEquals(indexAlias + "1", indexName);
        verify(indicesClient).delete(ArgumentMatchers.<Function<DeleteIndexRequest.Builder, ObjectBuilder<DeleteIndexRequest>>>any());
    }

    @Test
    void testCreateBboxIndexThatDoesntHaveAlias_DeleteIt() throws Exception {
        when(indicesClient.existsAlias(ArgumentMatchers.<Function<ExistsAliasRequest.Builder, ObjectBuilder<ExistsAliasRequest>>>any())).thenReturn(new BooleanResponse(false));
        when(indicesClient.exists(ArgumentMatchers.<Function<ExistsRequest.Builder, ObjectBuilder<ExistsRequest>>>any())).thenReturn(new BooleanResponse(true));

        ElasticsearchHelper.createBBoxIndex(esClient, indexAlias, supportedLanguages);

        verify(indicesClient).delete(ArgumentMatchers.<Function<DeleteIndexRequest.Builder, ObjectBuilder<DeleteIndexRequest>>>any());
    }
}