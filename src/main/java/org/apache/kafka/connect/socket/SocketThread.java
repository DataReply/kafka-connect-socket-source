package org.apache.kafka.connect.socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Thread to handle a single Socket connection
 *
 * @author Andrea Patelli
 */
public class SocketThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(SocketThread.class);
    private Socket clientSocket;
    private ConcurrentLinkedQueue<String> messages;

    public SocketThread(Socket clientSocket, ConcurrentLinkedQueue<String> messages) {
        this.clientSocket = clientSocket;
        this.messages = messages;
    }

    public void run() {
        InputStream input;
        BufferedReader br;
        try {
            // get the input stream
            input = clientSocket.getInputStream();
            br = new BufferedReader(new InputStreamReader(input));
        } catch (IOException e) {
            log.error(e.getMessage());
            return;
        }

        // while connected, reads a line and saves it in the queue
        String line;
        while (true) {
            try {
                line = br.readLine();
                if (line == null) {
                    clientSocket.close();
                    return;
                } else {
                    messages.add(line);
                }
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
    }
}
