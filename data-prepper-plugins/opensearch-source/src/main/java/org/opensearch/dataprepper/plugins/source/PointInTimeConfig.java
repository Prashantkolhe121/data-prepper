package org.opensearch.dataprepper.plugins.source;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
public class PointInTimeConfig {
    @JsonProperty("keep_alive")
    private String keepAlive;
    @JsonProperty("prefrence")
    @JsonIgnore
    private String prefrence;
    @JsonProperty("routing")
    @JsonIgnore
    private String routing;
    @JsonProperty("search_after")
    private String searchAfter;
    @JsonProperty("size")
    @JsonIgnore
    private String size;
    @JsonProperty("sort")
    private SortData sortData;
    public SortData getSortData() {
        return sortData;
    }
    public String getPrefrence() {
        return prefrence;
    }
    public String getRouting() {
        return routing;
    }
    public String getKeepAlive() {
        return keepAlive;
    }
    public String getSearchAfter() {
        return searchAfter;
    }
    public String getSize() {
        return size;
    }
}
