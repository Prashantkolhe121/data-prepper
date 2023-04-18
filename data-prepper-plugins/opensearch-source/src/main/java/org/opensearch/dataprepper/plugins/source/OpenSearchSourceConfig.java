package org.opensearch.dataprepper.plugins.source;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
public class OpenSearchSourceConfig {
    @JsonProperty("host")
    @NotNull
    @Valid
    private String host;
    @JsonProperty("query")
    private String query;
    @JsonProperty("datasource")
    private String datasource;
    @JsonProperty("include_indexes")
    private String includeIndexes;
    @JsonProperty("exclude_indexes")
    private String excludeIndexes;
    @JsonProperty("slice-scroll")
    private SliceScroll sliceScroll;
    @JsonProperty("point-in-time")
    private PointInTimeConfig pointInTimeConfig;
    @JsonProperty("max_retry")
    private String maxRetry;
    @JsonProperty("metrics")
    @JsonIgnore
    private String metrics;
    @JsonProperty("expression")
    @JsonIgnore
    private String expression;
    @JsonProperty("schedule")
    private String schedule;
    @JsonProperty("size")
    @JsonIgnore
    private String size;
    public String getSize() {
        return size;
    }
    public SliceScroll getSliceScroll() {
        return sliceScroll;
    }
    public String getQuery() {
        return query;
    }
    public String getHost() {
        return host;
    }
    public String getDatasource() {
        return datasource;
    }
    public String getIncludeIndexes() {
        return includeIndexes;
    }
    public String getExcludeIndexes() {
        return excludeIndexes;
    }
    public PointInTimeConfig getPointInTimeConfig() {
        return pointInTimeConfig;
    }
    public String getMaxRetry() {
        return maxRetry;
    }
    public String getMetrics() {
        return metrics;
    }
    public String getExpression() {
        return expression;
    }
    public String getSchedule() {
        return schedule;
    }
}
