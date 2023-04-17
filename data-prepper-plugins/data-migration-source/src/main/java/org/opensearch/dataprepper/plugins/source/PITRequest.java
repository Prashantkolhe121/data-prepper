package org.opensearch.dataprepper.plugins.source;

import jakarta.json.stream.JsonGenerator;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.JsonpSerializable;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;

import java.util.HashMap;
import java.util.Map;

public class PITRequest implements JsonpSerializable {

    private String index;

    private String keep_alive;



    public PITRequest(PITBuilder builder) {
        this.index = "movies";
        this.keep_alive = "1m";
    }

    public Map<String,String> queryParameters = new HashMap<>();

    public void setQueryParameters(Map<String, String> queryParameters) {
        this.queryParameters = queryParameters;
    }

    public Map<String, String> getQueryParameters() {
        return queryParameters;
    }

    private Map<String,String> params = new HashMap<>();

    public void setParams(Map<String, String> params) {
        this.params = params;
    }
    public static final Endpoint<PITRequest, PITResponse, ErrorResponse> ENDPOINT =
            new SimpleEndpoint<>(
                    r -> "POST",
                    r -> "http://localhost:9200/"+r.getIndex() +"/_search/point_in_time",
                    r-> r.getQueryParameters(),
                    SimpleEndpoint.emptyMap(),
                    true,
                    PITResponse.PARSER
            );

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getKeep_alive() {
        return keep_alive;
    }

    public void setKeep_alive(String keep_alive) {
        this.keep_alive = keep_alive;
    }

    @Override
    public void serialize(JsonGenerator generator, JsonpMapper mapper) {
        generator.writeStartObject();
    }
}
