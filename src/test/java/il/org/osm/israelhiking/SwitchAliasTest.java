package il.org.osm.israelhiking;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.ObjectBuilder;

/**
 * switchAlias must promote the freshly built target index without an atomic-rejection on the
 * first-ever build. Earlier it always issued remove(index="*").alias(alias) + add(...); when no
 * index carried the alias yet, that remove could fail the whole atomic updateAliases and leave the
 * new index unaliased. Now the remove is gated on the alias existing.
 */
@Tag("unit")
@ExtendWith(MockitoExtension.class)
class SwitchAliasTest {

    @Mock
    private ElasticsearchClient esClient;

    @Mock
    private ElasticsearchIndicesClient indicesClient;

    private final String alias = "points";
    private final String target = "points1";

    @BeforeEach
    void setUp() throws Exception {
        when(esClient.indices()).thenReturn(indicesClient);
        when(indicesClient.updateAliases(any(java.util.function.Function.class)))
                .thenReturn(UpdateAliasesResponse.of(b -> b.acknowledged(true)));
    }

    private List<Action> captureActions() throws Exception {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Function<UpdateAliasesRequest.Builder, ObjectBuilder<UpdateAliasesRequest>>> captor =
                ArgumentCaptor.forClass(Function.class);
        verify(indicesClient).updateAliases(captor.capture());
        return captor.getValue().apply(new UpdateAliasesRequest.Builder()).build().actions();
    }

    @Test
    void firstBuild_noPreexistingAlias_issuesAddOnly() throws Exception {
        when(indicesClient.existsAlias(any(java.util.function.Function.class)))
                .thenReturn(new BooleanResponse(false));

        assertDoesNotThrow(() -> ElasticsearchHelper.switchAlias(esClient, alias, target));

        List<Action> actions = captureActions();
        assertTrue(actions.stream().anyMatch(Action::isAdd), "the new index must be added to the alias");
        assertFalse(actions.stream().anyMatch(Action::isRemove),
                "no remove must be issued when the alias does not exist yet (avoids an atomic reject)");
    }

    @Test
    void rotation_aliasExists_removesOldThenAdds() throws Exception {
        when(indicesClient.existsAlias(any(java.util.function.Function.class)))
                .thenReturn(new BooleanResponse(true));

        assertDoesNotThrow(() -> ElasticsearchHelper.switchAlias(esClient, alias, target));

        List<Action> actions = captureActions();
        assertEquals(1, actions.stream().filter(Action::isRemove).count(),
                "an existing alias must be removed from its old index before the swap");
        assertTrue(actions.stream().anyMatch(Action::isAdd), "the new index must be added to the alias");
    }
}
