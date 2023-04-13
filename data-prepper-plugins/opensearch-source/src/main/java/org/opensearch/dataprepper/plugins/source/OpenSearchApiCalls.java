package org.opensearch.dataprepper.plugins.source;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
public class OpenSearchApiCalls implements SearchAPICalls {
    private  int maxRetry;
    public static final String GET_REQUEST_MEHTOD = "GET";
    public static final String POST_REQUEST_MEHTOD = "POST";
    public static final String POINT_IN_TIME_KEEP_ALIVE = "keep_alive";
    public static final String POINT_IN_TIME_PREFRENCE = "preference";
    public static final String POINT_IN_TIME_ROUTING = "routing";
    public static final String POINT_IN_TIME_EXPAND_WILDCARDS = "expand_wildcards";
    public static final String SEARCH_ENDPOINT = "/_search";

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchApiCalls.class);
    @Override
    public   String generatePitId(OpenSearchSourceConfig openSearchSourceConfig, RestHighLevelClient client) {

        Response response;
        String pitId = null;
        if(openSearchSourceConfig.getMaxRetry()!=null)
            maxRetry = Integer.parseInt(openSearchSourceConfig.getMaxRetry());
        String endPoint = openSearchSourceConfig.getIncludeIndexes() + "/_search/point_in_time";
        if (openSearchSourceConfig.getPointInTimeConfig().getKeepAlive() == null) {
            throw new IllegalArgumentException("Keep_alive is mandatory");
        }
        Request request = new Request(POST_REQUEST_MEHTOD, endPoint);
        request.addParameter(POINT_IN_TIME_KEEP_ALIVE, openSearchSourceConfig.getPointInTimeConfig().getKeepAlive());
       /* request.addParameter(POINT_IN_TIME_PREFRENCE,openSearchSourceConfig.getPointInTimeConfig().getPrefrence());
        request.addParameter(POINT_IN_TIME_ROUTING,openSearchSourceConfig.getPointInTimeConfig().getRouting() );
        request.addParameter(POINT_IN_TIME_EXPAND_WILDCARDS,openSearchSourceConfig.getExpression()!=null ? openSearchSourceConfig.getExpression() : "open" ) */      JSONParser jsonParser = new JSONParser();
        LOG.info("Requet is : {} ",request);
        for (int retry = 0; retry < maxRetry; retry++) {
            LOG.info("Trial count :  {} " , retry);
            try {
                response = client.getLowLevelClient().performRequest(request);
                LOG.info("Response is : {} ",response);
                String responseBody = EntityUtils.toString(response.getEntity());
                JSONObject jsonObject = (JSONObject) jsonParser.parse(responseBody);
                pitId = (String) jsonObject.get("pit_id");
                openSearchSourceConfig.getPointInTimeConfig().setPitId(pitId);
                LOG.info("Pit-id is : {} " , pitId);
                break;
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
                if (retry == maxRetry) {
                    try {
                        throw ex;
                    } catch (IOException | ParseException e) {
                        LOG.error("Max count of Retry is  {} ",retry,e);
                    }
                }
            }
        }
        return pitId;
    }

    @Override
    public String searchPitIndexes(String pitId, OpenSearchSourceConfig openSearchSourceConfig, RestHighLevelClient client) {
        Request getSearchRequest = new Request(GET_REQUEST_MEHTOD, SEARCH_ENDPOINT);
        Map<String, String> parameters = new HashMap<>();
        String getResponseBody = null;
        Response getResponse ;
        if (pitId == null) {
            throw new IllegalArgumentException("Pit_id is mandatory and not null");
        }
      //  parameters.put("size", openSearchSourceConfig.getPointInTimeConfig().getSize());
      //  parameters.put("query",openSearchSourceConfig.getExpression());
        parameters.put("id", openSearchSourceConfig.getPointInTimeConfig().getPitId());
        parameters.put(POINT_IN_TIME_KEEP_ALIVE, openSearchSourceConfig.getPointInTimeConfig().getKeepAlive());
        //parameters.put("sort",openSearchSourceConfig.getPointInTimeConfig().getSortData().getOrder());
        getSearchRequest.addParameters(parameters);
        LOG.info("Request is : {} ",getSearchRequest);
        try {
            getResponse = client.getLowLevelClient().performRequest(getSearchRequest);
            getResponseBody = EntityUtils.toString(getResponse.getEntity());
        } catch (IOException e) {
            LOG.error("Failed to SearchPitIndexes {} " , getResponseBody,e);
        }
        return getResponseBody;
    }
    @Override
    public String generateScrollId(OpenSearchSourceConfig openSearchSourceConfig, RestHighLevelClient client) {
        Response response;
        String scrollId = null;
        String endPoint = openSearchSourceConfig.getIncludeIndexes() + SEARCH_ENDPOINT;
        Request requestSroll = new Request(GET_REQUEST_MEHTOD, endPoint);
        requestSroll.addParameter("scroll", openSearchSourceConfig.getSliceScroll().getScroll());
        requestSroll.addParameter("slice_id", openSearchSourceConfig.getSliceScroll().getSliceId());
        requestSroll.addParameter("slice_max",openSearchSourceConfig.getSliceScroll().getSliceMax());

        LOG.info("Request Endpoint :  {} " , requestSroll.getEndpoint());
        JSONParser jsonParser = new JSONParser();
        for (int retry = 0; retry < maxRetry; retry++) {
            LOG.info("Trial count :  {} " , retry);
            try {
                response = client.getLowLevelClient().performRequest(requestSroll);
                String responseBody = EntityUtils.toString(response.getEntity());
                JSONObject jsonObject = (JSONObject) jsonParser.parse(responseBody);
                LOG.info("Response of get Scroll id : {} " , responseBody);
                scrollId = (String) jsonObject.get("_scroll_id");
                break;
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
            }
        }
        return scrollId;
    }

    @Override
    public String searchScrollIndexes(OpenSearchSourceConfig openSearchSourceConfig, RestHighLevelClient client) {
        Response response = null;
        String responseBody = null;
        String endPoint = openSearchSourceConfig.getIncludeIndexes() + SEARCH_ENDPOINT;
        Request requestSroll = new Request(GET_REQUEST_MEHTOD, endPoint);
        requestSroll.addParameter("slice_id", openSearchSourceConfig.getSliceScroll().getSliceId());
        requestSroll.addParameter("slice_max", openSearchSourceConfig.getSliceScroll().getSliceMax());
        requestSroll.addParameter("query_match_all", openSearchSourceConfig.getExpression());
        try {
            response = client.getLowLevelClient().performRequest(requestSroll);
            responseBody = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            LOG.error("Response of searchScrollIndexes() is failed : {}  " , response , e);
        }
        return responseBody;
    }
}
