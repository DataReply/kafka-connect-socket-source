package org.apache.kafka.connect.socket;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Andrea Patelli on 12/02/2016.
 */
public class SocketSourceConnector extends SourceConnector {
    private final static Logger log = LoggerFactory.getLogger(SocketSourceConnector.class);

    public static final String PORT = "port";
    public static final String SCHEMA_NAME = "schema.name";
    public static final String BATCH_SIZE = "batch.size";
    public static final String TOPIC = "topic";

    private String port;
    private String schemaName;
    private String batchSize;
    private String topic;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        log.trace("Parsing configuration");

        port = map.get(PORT);
        if (port == null || port.isEmpty())
            throw new ConnectException("Missing " + PORT + " config");

        schemaName = map.get(SCHEMA_NAME);
        if (schemaName == null || schemaName.isEmpty())
            throw new ConnectException("Missing " + SCHEMA_NAME + " config");

        batchSize = map.get(BATCH_SIZE);
        if (batchSize == null || batchSize.isEmpty())
            throw new ConnectException("Missing " + BATCH_SIZE + " config");

        topic = map.get(TOPIC);
        if (topic == null || topic.isEmpty())
            throw new ConnectException("Missing " + TOPIC + " config");

        dumpConfiguration(map);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SocketSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(PORT, port);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
    }

    private void dumpConfiguration(Map<String, String> map) {
        log.trace("Starting connector with configuration:");
        for (Map.Entry entry : map.entrySet()) {
            log.trace("{}: {}", entry.getKey(), entry.getValue());
        }
    }
}
