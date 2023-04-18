package org.opensearch.dataprepper.plugins.source;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@DataPrepperPlugin(name = "opensearchsource", pluginType = Source.class, pluginConfigurationType =OpenSearchSourceConfig.class)
public class OpenSearchSource implements Source<Record<Event>> {

    private final OpenSearchSourceConfig openSearchSourceConfig;

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSource.class);

    @DataPrepperPluginConstructor
    public OpenSearchSource(OpenSearchSourceConfig openSearchSourceConfig) {
        this.openSearchSourceConfig = openSearchSourceConfig;
    }
    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        callToApis(openSearchSourceConfig);
    }
    private void callToApis(OpenSearchSourceConfig openSearchSourceConfig)  {
        try {
            String datasource;
            SourceInfo sourceInfo = new SourceInfo();
            SourceInfoProvider sourceInfoProvider= new SourceInfoProvider();
            datasource = sourceInfoProvider.getsourceInfo(openSearchSourceConfig);
            sourceInfo.setDataSource(datasource);
            LOG.info("Datasource is : {} " , sourceInfo);
            sourceInfo = sourceInfoProvider.checkStatus(openSearchSourceConfig,sourceInfo);

            if (Boolean.TRUE.equals(sourceInfo.getHealthStatus())) {
                PrepareConnection prepareConnection = new PrepareConnection();
                ElasticsearchClient client = prepareConnection.restHighprepareOpensearchConnection();
                sourceInfoProvider.versionCheck(openSearchSourceConfig,sourceInfo,client);
            }
            else {
                LOG.info("Retry after sometime");
            }
        }
        catch (Exception e)
        {
            LOG.error("Exception occur : ",e);
        }
    }
    @Override
    public void stop() {

    }
}