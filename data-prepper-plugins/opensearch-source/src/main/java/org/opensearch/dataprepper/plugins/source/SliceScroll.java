package org.opensearch.dataprepper.plugins.source;
import com.fasterxml.jackson.annotation.JsonProperty;
public class SliceScroll {
    @JsonProperty("scroll")
    private String scroll;
    @JsonProperty("slice_max")
    private String sliceMax;
    @JsonProperty("slice_id")
    private String sliceId;
    @JsonProperty("size")
    private String size;
    public String getSize() {
        return size;
    }
    public void setSize(String size) {
        this.size = size;
    }
    public String getScroll() {
        return scroll;
    }
    public String getSliceMax() {
        return sliceMax;
    }
    public String getSliceId() {
        return sliceId;
    }
}