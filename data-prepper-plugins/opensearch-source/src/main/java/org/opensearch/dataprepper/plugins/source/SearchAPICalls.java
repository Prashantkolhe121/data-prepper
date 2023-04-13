package org.opensearch.dataprepper.plugins.source;
import org.opensearch.client.RestHighLevelClient;
public interface SearchAPICalls {
      String generatePitId(OpenSearchSourceConfig openSearchSourceConfig, RestHighLevelClient client);
      String searchPitIndexes(String pitId, OpenSearchSourceConfig openSearchSourceConfig, RestHighLevelClient client);
      String generateScrollId(OpenSearchSourceConfig openSearchSourceConfig, RestHighLevelClient client);
      String searchScrollIndexes(OpenSearchSourceConfig openSearchSourceConfig, RestHighLevelClient client);
}
