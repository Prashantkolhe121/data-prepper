package org.opensearch.dataprepper.plugins.source;

import org.json.simple.JSONObject;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;
import java.util.HashMap;
import java.util.Map;
public class ScrollRequest {

    private String size;

    private String scroll;

    private JSONObject jsonData;

    public JSONObject getJsonData() {
        return jsonData;
    }

    public void setJsonData(JSONObject jsonData) {
        this.jsonData = jsonData;
    }

    public ScrollRequest(ScrollBuilder builder) {

        this.size = size;
    }

    public String getScroll() {
        return scroll;
    }

    public void setScroll(String scroll) {
        this.scroll = scroll;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public static final Endpoint<ScrollRequest, ScrollResponse, ErrorResponse> ENDPOINT =
            new SimpleEndpoint<>(
                    r -> "GET",
                    r -> "http://localhost:9200/" + "movies" + "/_search?scroll=10m",
                    SimpleEndpoint.emptyMap(),
                    SimpleEndpoint.emptyMap(),
                    true,
                    ScrollResponse.PARSER
            );

}