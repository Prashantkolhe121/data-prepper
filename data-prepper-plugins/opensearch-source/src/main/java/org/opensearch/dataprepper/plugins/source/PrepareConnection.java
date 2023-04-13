package org.opensearch.dataprepper.plugins.source;
import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
public class PrepareConnection {
   public RestHighLevelClient restHighprepareOpensearchConnection(){

      RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
              .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder);
      return new RestHighLevelClient(builder);
   }
}


