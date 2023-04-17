package org.opensearch.dataprepper.plugins.source;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@DataPrepperPlugin(name = "opensearch", pluginType = Source.class, pluginConfigurationType = DataMigrationSourceConfig.class)
public class DataMigrationSource implements Source<Record<Event>> {

    private final DataMigrationSourceConfig dataMigrationSourceConfig;

    private String osVersion;

    private String datasource;

    private Boolean clusterStatus;

    private Transport transport;

    private static final Logger LOG = LoggerFactory.getLogger(DataMigrationSource.class);


    private final PluginMetrics pluginMetrics;

    private static final int MAX_RETRIES = 3;

    @DataPrepperPluginConstructor
    public DataMigrationSource(final PluginMetrics pluginMetrics, final DataMigrationSourceConfig dataMigrationSourceConfig, final PluginFactory pluginFactory) {
        this.pluginMetrics = pluginMetrics;
        this.dataMigrationSourceConfig = dataMigrationSourceConfig;

    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {

        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        try {
            datasource = getDataSource(dataMigrationSourceConfig);
            clusterStatus = getClusterStats(dataMigrationSourceConfig);
            if (clusterStatus) {
                checkApi(dataMigrationSourceConfig,datasource,osVersion);
                   } else {
                LOG.info("retry code");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void checkApi(DataMigrationSourceConfig dataMigrationSourceConfig, String datasource, String osVersion) throws IOException {

        if(datasource!=null && datasource.equalsIgnoreCase("opensearch"))
        {
          OpenSearchClient client =  opensearchClientConnection(dataMigrationSourceConfig);
          System.out.println("Search Data : ");
          //trying to call to PIT API By using SearchResponse
            SearchResponse<ObjectNode> searchResponse = client.search(
                    b -> b.index(dataMigrationSourceConfig.getIndex()), ObjectNode.class);
            for (int i = 0; i < searchResponse.hits().hits().size(); i++) {
                System.out.println(searchResponse.hits().hits().get(i).source());
            }

            //by using _transport().performRequest()
            System.out.println("---------------------PIT ID Using Java Client----------------");
            PITRequest pitRequest = new PITRequest(new PITBuilder());
            Map<String,String> params = new HashMap<>();
            params.put("keep_alive","1m");
            pitRequest.setQueryParameters(params);
            System.out.println("pitRequest  : " + pitRequest==null);
            System.out.println("end point  : " +  PITRequest.ENDPOINT.requestUrl(pitRequest));
            System.out.println("query para : " + PITRequest.ENDPOINT.queryParameters(pitRequest));
            PITResponse  pitResponse =  client._transport().performRequest(pitRequest,PITRequest.ENDPOINT,client._transportOptions());
            System.out.println("PIT Response is :  " + pitResponse);

        }
    }

    private String getDataSource(DataMigrationSourceConfig dataMigrationSourceConfig) throws IOException, ParseException {

        URL obj = new URL(dataMigrationSourceConfig.getHosts());
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("content-type", "application/json");
        int responseCode = con.getResponseCode();
        JSONParser jsonParser = new JSONParser();
        StringBuffer response = new StringBuffer();

        System.out.println("GET Response Code for Data Source :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            System.out.println(response.toString());
        } else {
            System.out.println("GET request did not work.");
        }
        JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
        Map version = ((Map) jsonObject.get("version"));
        Iterator<Map.Entry> itrator = version.entrySet().iterator();

        while (itrator.hasNext()) {
            Map.Entry pair = itrator.next();
            if (pair.getKey().equals("distribution")) {
                datasource = String.valueOf(pair.getValue());
            }
        }
        System.out.println("Data Source Name is :  " + datasource);
        if(datasource == null)
            datasource = "Elasticsearch";
        return datasource;
    }

    private boolean getClusterStats(DataMigrationSourceConfig dataMigrationSourceConfig) throws IOException, ParseException {

        URL obj = new URL(dataMigrationSourceConfig.getHosts() + "_cluster/stats");
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("content-type", "application/json");
        int responseCode = con.getResponseCode();
        JSONParser jsonParser = new JSONParser();
        StringBuffer response = new StringBuffer();
        String status;

        System.out.println("GET Response Code for Cluster Status :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            System.out.println(response.toString());
        } else {
            System.out.println("GET request did not work.");
        }
        JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
        status = (String) jsonObject.get("status");
        System.out.println(status);

        if (status.equalsIgnoreCase("red"))
            return false;
        Map nodes = ((Map) jsonObject.get("nodes"));
        Iterator<Map.Entry> itrator = nodes.entrySet().iterator();

        while (itrator.hasNext()) {
            Map.Entry pair = itrator.next();
            if (pair.getKey().equals("versions")) {
                osVersion = String.valueOf(pair.getValue());
            }
        }
        System.out.println("Open Search version " + osVersion);
        return true;
    }

    public OpenSearchClient opensearchClientConnection(DataMigrationSourceConfig dataMigrationSourceConfig) throws IOException {

        final HttpHost host = new HttpHost("http", "localhost", 9200);
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        final OpenSearchTransport transport = ApacheHttpClient5TransportBuilder
                .builder(host)
                .setMapper(new JacksonJsonpMapper())
                //    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_2))
                .build();
        OpenSearchClient osClient = new OpenSearchClient(transport);
         return osClient;
    }
    @Override
    public void stop() {

    }
}
