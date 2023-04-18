package org.opensearch.dataprepper.plugins.source;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

public interface SearchAPICalls {
    String generatePitId(OpenSearchSourceConfig openSearchSourceConfig, ElasticsearchClient client);

    String searchPitIndexes(String pitId, OpenSearchSourceConfig openSearchSourceConfig, ElasticsearchClient client);

    String generateScrollId(OpenSearchSourceConfig openSearchSourceConfig, ElasticsearchClient client);

    String searchScrollIndexes(OpenSearchSourceConfig openSearchSourceConfig, ElasticsearchClient client);
}
