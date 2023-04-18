package org.opensearch.dataprepper.plugins.source;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpResponseException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
public class SourceInfoProvider {
    private String datasource;
    private static final Logger LOG = LoggerFactory.getLogger(SourceInfoProvider.class);
    private static final String GET_REQUEST_MEHTOD = "GET";
    private static final String CONTENT_TYPE = "content-type";
    private static final String CONTENT_TYPE_VALUE = "application/json";
    private static final String VERSION = "version";
    private static final String DISTRIBUTION = "distribution";
    private static final String ELASTICSEARCH = "elasticsearch";
    private static final String CLUSTER_STATS_ENDPOINTS = "_cluster/stats";
    private static final String CLUSTER_HEALTHSTATUS = "status";
    private static final String CLUSTER_HEALTHSTATUS_RED = "red";
    private static final String NODES = "nodes";
    private static final String VERSIONS = "versions";
    private static final String REGULAR_EXPRESSION = "[^a-zA-Z0-9]";
    private static final int VERSION_7_10_0 = 7100;
    public String getsourceInfo(OpenSearchSourceConfig openSearchSourceConfig) {
        try {
            JSONParser jsonParser = new JSONParser();
            StringBuilder response = new StringBuilder();
            if (StringUtils.isBlank(openSearchSourceConfig.getHost()))
                throw new IllegalArgumentException("Hostname cannot be null or empty");
            URL obj = new URL(openSearchSourceConfig.getHost());
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod(GET_REQUEST_MEHTOD);
            con.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
            int responseCode = con.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                if (StringUtils.isBlank(response.toString()))
                    throw new HttpResponseException(HttpStatus.SC_GATEWAY_TIMEOUT, "Server response is not received");
                LOG.info("Response is  : {} ", response);
            } else {
                LOG.error("GET request did not work.");
            }
            JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
            Map<String, String> versionMap = ((Map) jsonObject.get(VERSION));
            for (Map.Entry<String, String> entry : versionMap.entrySet()) {
                if (entry.getKey().equals(DISTRIBUTION)) {
                    datasource = String.valueOf(entry.getValue());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (datasource == null)
            datasource = ELASTICSEARCH;
        return datasource;
    }
    public SourceInfo checkStatus(OpenSearchSourceConfig openSearchSourceConfig, SourceInfo sourceInfo) throws IOException, ParseException {
        String osVersion = null;
        URL obj = new URL(openSearchSourceConfig.getHost() + CLUSTER_STATS_ENDPOINTS);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod(GET_REQUEST_MEHTOD);
        con.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
        int responseCode = con.getResponseCode();
        JSONParser jsonParser = new JSONParser();
        StringBuilder response = new StringBuilder();
        String status;
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            LOG.info("Response is {} ", response);
        } else {
            LOG.info("GET request did not work.");
        }
        JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
        status = (String) jsonObject.get(CLUSTER_HEALTHSTATUS);
        if (status.equalsIgnoreCase(CLUSTER_HEALTHSTATUS_RED))
            sourceInfo.setHealthStatus(false);
        Map<String, String> nodesMap = ((Map) jsonObject.get(NODES));
        for (Map.Entry<String, String> entry : nodesMap.entrySet()) {
            if (entry.getKey().equals(VERSIONS)) {
                osVersion = String.valueOf(entry.getValue());
                sourceInfo.setOsVersion(osVersion);
            }
        }
        LOG.info("Open Search version  : {} ", osVersion);
        return sourceInfo;
    }
    public void versionCheck(OpenSearchSourceConfig openSearchSourceConfig, SourceInfo sourceInfo, ElasticsearchClient client) {
        int osVersionIntegerValue = Integer.parseInt(sourceInfo.getOsVersion().replaceAll(REGULAR_EXPRESSION, ""));
        if ((sourceInfo.getDataSource().equalsIgnoreCase(ELASTICSEARCH))
                && (osVersionIntegerValue >= VERSION_7_10_0)) {
            ElasticSearchApiCalls elasticSearchApiCalls = new ElasticSearchApiCalls();
            String pitId = elasticSearchApiCalls.generatePitId(openSearchSourceConfig, client);
            LOG.info("Pit Id is  {} ", pitId);
            String getSearchResponseBody = elasticSearchApiCalls.searchPitIndexes(pitId, openSearchSourceConfig, client);
            LOG.info("Search After Response :{} ", getSearchResponseBody);
        } else if (sourceInfo.getDataSource().equalsIgnoreCase(ELASTICSEARCH) && (osVersionIntegerValue < VERSION_7_10_0)) {
            ElasticSearchApiCalls elasticSearchApiCalls = new ElasticSearchApiCalls();
            String scrollId = elasticSearchApiCalls.generateScrollId(openSearchSourceConfig, client);
            LOG.info("Scroll Id : {} ", scrollId);
            try(PrintWriter out = new PrintWriter(new FileWriter("D:\\scrolldata1"))) {
                ScrollRequest scrollRequest = elasticSearchApiCalls.nextScrollRequest(scrollId);
                ScrollResponse<ObjectNode> searchResponse1 = client.scroll(scrollRequest, ObjectNode.class);
                out.write(searchResponse1.toString());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
