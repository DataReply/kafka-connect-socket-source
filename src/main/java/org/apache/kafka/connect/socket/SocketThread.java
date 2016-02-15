package org.apache.kafka.connect.socket;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * SocketThread accepts connections from a Socket and saves messages on a queue.
 *
 * @author Andrea Patelli
 */
public class SocketThread implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SocketThread.class);
    public ConcurrentLinkedQueue<String> messages;

    private Integer port;

    private ServerSocket serverSocket;
    private BufferedReader input;
    private Socket clientSocket;

    /**
     * Constructor of the class.
     *
     * @param port to use for creating the Socket
     */
    public SocketThread(Integer port) {
        this.port = port;
        this.messages = new ConcurrentLinkedQueue<>();
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new ConnectException("Impossible to open socket on port " + port);
        }
    }

    /**
     * Run the thread.
     */
    @Override
    public void run() {
        try {
            if (clientSocket == null)
                connect();
            // forever
            while (true) {
                String line;
                // while the connection is open
                while ((line = input.readLine()) != null) {
                    // add new messages to the queue
                    messages.add(line);
                }
                // if disconnected, wait for new connections
                connect();
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void connect() throws IOException {
        // accept connections
        Socket newClient = serverSocket.accept();
        clientSocket = newClient;
        // create new input stream
        input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    }

    public void stop() {
        try {
            serverSocket.close();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
