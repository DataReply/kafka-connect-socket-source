package org.apache.kafka.connect.socket;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

/**
 * Created by Andrea Patelli on 12/02/2016.
 */
public class SocketSourceTaskTest {
    Logger log = LoggerFactory.getLogger(SocketSourceTaskTest.class);
    private SocketSourceTask task;

    @Before
    public void setup() {
        task = new SocketSourceTask();
        Map<String, String> configs = new HashMap<>();
        configs.put(SocketSourceConnector.PORT, "12345");
        configs.put(SocketSourceConnector.SCHEMA_NAME, "schematest");
        configs.put(SocketSourceConnector.BATCH_SIZE, "100");
        configs.put(SocketSourceConnector.TOPIC, "topic");
        task.start(configs);
    }

    @Test
    public void testEmpty() throws InterruptedException {
        List<SourceRecord> records = task.poll();
        org.junit.Assert.assertNotNull(records);
        org.junit.Assert.assertEquals(0, records.size());
    }

    @Test
    public void testLessThanBatchSize() throws Exception {
        Socket producer = new Socket("localhost", 12345);
        PrintWriter out = new PrintWriter(producer.getOutputStream(), true);
        List<String> original = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            String s = UUID.randomUUID().toString();
            out.println(s);
            original.add(s);
        }
        List<SourceRecord> records = task.poll();
        org.junit.Assert.assertEquals(original.size(), records.size());
        for (int i = 0; i < records.size(); i++) {
            String actual = ((Struct) records.get(i).value()).get("message").toString();
            String expected = original.get(i);
            org.junit.Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testMoreThanBatchSize() throws Exception {
        Socket producer = new Socket("localhost", 12345);
        PrintWriter out = new PrintWriter(producer.getOutputStream(), true);
        List<String> original = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            String s = UUID.randomUUID().toString();
            out.println(s);
            original.add(s);
        }

        List<SourceRecord> records = new ArrayList<>();
        List<SourceRecord> poll;
        do {
            poll = task.poll();
            records.addAll(poll);
        } while (poll.size() > 0);

        org.junit.Assert.assertEquals(original.size(), records.size());

        for (int i = 0; i < records.size(); i++) {
            String actual = ((Struct) records.get(i).value()).get("message").toString();
            String expected = original.get(i);
            org.junit.Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testWithDisconnection() throws Exception {
        int count = 0;
        Socket producer = new Socket("localhost", 12345);
        PrintWriter out = new PrintWriter(producer.getOutputStream(), true);
        List<String> original = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            String s = UUID.randomUUID().toString();
            out.println(s);
            original.add(s);
        }

        // disconnect
        out.close();
        producer.close();

        // reconnect and write again
        producer = new Socket("localhost", 12345);
        out = new PrintWriter(producer.getOutputStream(), true);
        for (int i = 0; i < 10000; i++) {
            String s = UUID.randomUUID().toString();
            out.println(s);
            original.add(s);
        }

        List<SourceRecord> records = new ArrayList<>();
        List<SourceRecord> poll;
        do {
            poll = task.poll();
            records.addAll(poll);
        } while (poll.size() > 0);

        org.junit.Assert.assertEquals(original.size(), records.size());

        for (int i = 0; i < records.size(); i++) {
            String actual = ((Struct) records.get(i).value()).get("message").toString();
            String expected = original.get(i);
            org.junit.Assert.assertEquals(expected, actual);
        }
    }


    @After
    public void close() {
        task.stop();
    }
}
