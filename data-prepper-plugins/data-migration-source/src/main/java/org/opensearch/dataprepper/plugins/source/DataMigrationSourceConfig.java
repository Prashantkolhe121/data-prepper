package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataMigrationSourceConfig {
    @JsonProperty("hosts")
    private String hosts;
    @JsonProperty("query")
    private String query;
    @JsonProperty("keep_alive")
    private String keep_alive;
    @JsonProperty("datasource")
    private String datasource;
    @JsonProperty("index")
    private String index;

    @JsonProperty("scroll")
    private String scroll;
    @JsonProperty("size")
    private String size;

    public String getScroll() {
        return scroll;
    }

    public String getSize() {
        return size;
    }

    public String getQuery() {
        return query;
    }

    public String getHosts() {
        return hosts;
    }

    public String getDatasource() {
        return datasource;
    }

    public String getKeep_alive() {
        return keep_alive;
    }

    public String getIndex() {
        return index;
    }
}