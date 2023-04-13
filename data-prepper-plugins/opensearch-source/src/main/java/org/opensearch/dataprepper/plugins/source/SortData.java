package org.opensearch.dataprepper.plugins.source;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
public class SortData {
    @JsonProperty("order")
    @JsonIgnore
    private String order;
    public String getOrder() {
        return order;
    }
}
