package org.opensearch.dataprepper.plugins.source;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.stream.Collectors;
public class ElasticSearchApiCalls implements SearchAPICalls {
    public static final String GET_REQUEST_MEHTOD = "GET";
    public static final String POST_REQUEST_MEHTOD = "POST";
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchApiCalls.class);
    private int maxRetry;
    @Override
    public String generatePitId(OpenSearchSourceConfig openSearchSourceConfig, ElasticsearchClient client) {
        OpenPointInTimeResponse response = null;
        if (openSearchSourceConfig.getMaxRetry() != null)
            maxRetry = Integer.parseInt(openSearchSourceConfig.getMaxRetry());
        if (openSearchSourceConfig.getPointInTimeConfig().getKeepAlive() == null)
            throw new IllegalArgumentException("Keep_alive is mandatory");
        OpenPointInTimeRequest request = new OpenPointInTimeRequest.Builder().
                index(openSearchSourceConfig.getIncludeIndexes()).
                keepAlive(new Time.Builder().time(openSearchSourceConfig.getPointInTimeConfig().getKeepAlive()).build()).build();
        LOG.info("Requet is : {} ", request);
        for (int retry = 0; retry < maxRetry; retry++) {
            LOG.info("Trial count :  {} ", retry);
            try {
                response = client.openPointInTime(request);
                break;
            } catch (IOException ex) {
                LOG.error(ex.getMessage());
                if (retry == maxRetry) {
                    try {
                        throw ex;
                    } catch (Exception e) {
                        LOG.error("Max count of Retry is  {} ", retry, e);
                    }
                }
            }
        }
        return response.id();
    }
    @Override
    public String searchPitIndexes(String pitId, OpenSearchSourceConfig openSearchSourceConfig, ElasticsearchClient client) {
        SearchResponse<ObjectNode> searchResponse = null;
        for (int retry = 0; retry < maxRetry; retry++) {
            LOG.info("Trial count :  {} ", retry);
            try {
                searchResponse = client.search(req ->
                                req.index(openSearchSourceConfig.getIncludeIndexes()),
                        ObjectNode.class);
                searchResponse.hits().hits().stream()
                        .map(Hit::source).collect(Collectors.toList());
                break;
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
                if (retry == maxRetry) {
                    try {
                        throw ex;
                    } catch (IOException e) {
                        LOG.error("Max count of Retry is  {} ", retry, e);
                    }
                }
            }
        }
        return searchResponse.toString();
    }
    @Override
    public String generateScrollId(OpenSearchSourceConfig openSearchSourceConfig, ElasticsearchClient client) {
        SearchResponse response = null;
        SearchRequest searchRequest = SearchRequest
                .of(e -> e.index(openSearchSourceConfig.getIncludeIndexes()).size(Integer.parseInt(openSearchSourceConfig.getSliceScroll().getSize())).scroll(scr -> scr.time("5m")));
        try {
            response = client.search(searchRequest, ObjectNode.class);
            LOG.info("Response is : {} ",response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return response.scrollId();
    }
    public ScrollRequest nextScrollRequest(final String scrollId) {
        return ScrollRequest
                .of(scrollRequest -> scrollRequest.scrollId(scrollId).scroll(Time.of(t -> t.time("1m"))));
    }
    @Override
    public String searchScrollIndexes(OpenSearchSourceConfig openSearchSourceConfig, ElasticsearchClient client) {
        return null;
    }
}