import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReplicationManager extends Thread {
    private Log logger = new Log(this.getClass().getName());

    private List<String> membership; // server id
    private int memberCount;
    private String serverID;
    private String serverAddress;
    private int serverPort;
    private InetSocketAddress serverSocket;
    private AsynchronousServerSocketChannel asyncServer;
    AsynchronousSocketChannel client;

    public static void main (String[] args) {
        ReplicationManager rm = new ReplicationManager();
        rm.serverID = args[0];
        rm.serverAddress = args[1];
        rm.serverPort = Integer.parseInt(args[2]);
        rm.membership = new ArrayList<String>();
        rm.memberCount = 0;
        rm.run();
    }

    @Override
    public void run() {
        this.serverSocket = new InetSocketAddress(serverAddress, serverPort);
        startRM();
    }

    private void startRM() {
        try (AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open()) {
            logger.log(Level.INFO, "Server " + serverID + " Running On IP " + serverAddress + " Port " + serverPort);
            logger.log(Level.INFO, "RM Having 0 Members:", "Member");
            server.bind(serverSocket);
            this.asyncServer = server;
            serverRunning();
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    private void serverRunning() throws IOException {
        try {
            while (true) {
                Future<AsynchronousSocketChannel> acceptCon = this.asyncServer.accept();
                client = acceptCon.get(300, TimeUnit.SECONDS);
                if ((client != null) && (client.isOpen())) {
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    Future<Integer> readval = client.read(buffer);
                    while (!readval.isDone() && !readval.isCancelled()) {
                        Thread.sleep(1000);
                    }
                    String message = new String(buffer.array()).trim();
                    readval.get();
                    buffer.flip();
                    logger.log(Level.INFO, "Received message " + message);
                    handleMessage(message);
                }

            }
        } catch (InterruptedException e) {
            logger.log(Level.INFO, "RM terminate");
            if (client != null){
                client.close();
            }
        } catch (TimeoutException e) {
            logger.log(Level.WARNING, e.getMessage());
            logger.log(Level.WARNING, "Timeout... Ending RM socket");
            if (client != null){
                client.close();
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            if (client != null){
                client.close();
            }
        }
    }

    private void handleMessage(String message) {
        String[] messageType = message.split(", ");
        if (messageType[2].contains("membership")) {
            logger.log(Level.INFO, "Received " + message, "RM");
            processMembershipMessage(messageType);
        }
    }

    private void processMembershipMessage(String[] messageType) {
        logger.log(Level.INFO, messageType[3], "RM");
        String str = messageType[4].replace(" ", "").replace(">", "");
        this.memberCount = Integer.parseInt(str);
    }
}
