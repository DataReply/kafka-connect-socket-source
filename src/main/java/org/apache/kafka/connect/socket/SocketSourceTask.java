package org.apache.kafka.connect.socket;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * SocketSourceTask is a Task that reads records from a Socket for storage in Kafka.
 *
 * @author Andrea Patelli
 */
public class SocketSourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(SocketSourceTask.class);

    private Integer port;
    private Integer batchSize = 100;
    private String schemaName;
    private String topic;
    private SocketThread socketThread;
    private static Schema schema = null;

    @Override
    public String version() {
        return null;
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the Task.
     *
     * @param map initial configuration
     */
    @Override
    public void start(Map<String, String> map) {
        try {
            port = Integer.parseInt(map.get(SocketSourceConnector.PORT));
        } catch (Exception e) {
            throw new ConnectException(SocketSourceConnector.PORT + " config should be an Integer");
        }

        try {
            batchSize = Integer.parseInt(map.get(SocketSourceConnector.BATCH_SIZE));
        } catch (Exception e) {
            throw new ConnectException(SocketSourceConnector.BATCH_SIZE + " config should be an Integer");
        }

        schemaName = map.get(SocketSourceConnector.SCHEMA_NAME);
        topic = map.get(SocketSourceConnector.TOPIC);

        log.trace("Creating schema");
        schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("message", Schema.OPTIONAL_STRING_SCHEMA);


        log.trace("Opening Socket");
        socketThread = new SocketThread(port);
        new Thread(socketThread).start();
    }

    /**
     * Poll this SocketSourceTask for new records.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>(0);
        // while there are new messages in the socket queue
        while (!socketThread.messages.isEmpty() && records.size() < batchSize) {
            // get the message
            String message = socketThread.messages.poll();
            // creates the structured message
            Struct messageStruct = new Struct(schema);
            messageStruct.put("message", message);
            // creates the record
            // no need to save offsets
            SourceRecord record = new SourceRecord(Collections.singletonMap("socket", 0), Collections.singletonMap("0", 0), topic, messageStruct.schema(), messageStruct);
            records.add(record);
        }
        return records;
    }

    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
        socketThread.stop();
    }
}
