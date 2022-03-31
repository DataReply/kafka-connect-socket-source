package org.apache.kafka.connect.socket;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * SocketSourceConnector implements the connector interface
 * to write on Kafka messages received on a Socket
 *
 * @author Andrea Patelli
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

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param map configuration settings
     */
    @Override
    public void start(Map<String, String> map) {
        log.trace("Parsing configuration");

        port = map.get(PORT);
        validateConfig(port, PORT);

        schemaName = map.get(SCHEMA_NAME);
        validateConfig(schemaName, SCHEMA_NAME);

        batchSize = map.get(BATCH_SIZE);
        validateConfig(batchSize, BATCH_SIZE);

        topic = map.get(TOPIC);
        validateConfig(topic, TOPIC);

        dumpConfiguration(map);
    }

    private void validateConfig(String config, String configKey) {
        if (config == null || config.isEmpty())
            throw new ConnectException("Missing " + configKey + " config");
    }

    /**
     * Returns the Task implementation for this Connector.
     *
     * @return tha Task implementation Class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return SocketSourceTask.class;
    }

    /**
     * Returns a set of configurations for the Task based on the current configuration.
     * It always creates a single set of configurations.
     *
     * @param i maximum number of configurations to generate
     * @return configurations for the Task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return List.of(Map.ofEntries(
                Map.entry(PORT, port),
                Map.entry(SCHEMA_NAME, schemaName),
                Map.entry(BATCH_SIZE, batchSize),
                Map.entry(TOPIC, topic)

        ));
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {

        return null;
    }

    private void dumpConfiguration(Map<String, String> map) {
        log.trace("Starting connector with configuration:");
        map.forEach((key, value) -> log.trace("{}: {}", key, value));
    }
}
