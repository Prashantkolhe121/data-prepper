package org.opensearch.dataprepper.plugins.source;

import org.opensearch.client.RestHighLevelClient;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

@DataPrepperPlugin(name = "opensearchsource", pluginType = Source.class, pluginConfigurationType =OpenSearchSourceConfig.class)
public class OpenSearchSource implements Source<Record<Event>> {
    private final OpenSearchSourceConfig openSearchSourceConfig;
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSource.class);
    private RestHighLevelClient client;
    private SourceInfoProvider sourceInfoProvider;
    private SourceInfo sourceInfo;
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
            sourceInfo = new SourceInfo();
            sourceInfoProvider= new SourceInfoProvider();
            datasource = sourceInfoProvider.getsourceInfo(openSearchSourceConfig);
            sourceInfo.setDataSource(datasource);
            LOG.info("Datasource is : {} " , sourceInfo);
            sourceInfo = sourceInfoProvider.checkStatus(openSearchSourceConfig,sourceInfo);

            if (Boolean.TRUE.equals(sourceInfo.getHealthStatus())) {
                PrepareConnection prepareConnection = new PrepareConnection();
                client = prepareConnection.restHighprepareOpensearchConnection();
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
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}