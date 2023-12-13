import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

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

    private String gfdAddress;
    private int gfdPort;
    
    private int heartbeatCount = 0;
    private int timeout = 5;
    private int heartbeatInterval = 5000;

    private double maxLatencyGFD = 0;
    private double minLatencyGFD = Double.POSITIVE_INFINITY;
    private double avgLatencyGFD = 0;
    private int messageCount = 0;

    private int logInterval = 10000;
    private String fileName = "../log/RMGFD.csv";
    private static final SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main (String[] args) {
        ReplicationManager rm = new ReplicationManager();
        rm.serverID = args[0];
        rm.serverAddress = args[1];
        rm.serverPort = Integer.parseInt(args[2]);
        rm.gfdAddress = args[3];
        rm.gfdPort = Integer.parseInt(args[4]);
        rm.membership = new ArrayList<String>();
        rm.memberCount = 0;
        rm.run();
    }

    @Override
    public void run() {
        this.serverSocket = new InetSocketAddress(serverAddress, serverPort);
        spawnLFDThread();
        spawnLogThread();
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

    private void spawnLFDThread(){
        Thread gfd = new Thread(() -> {
            logger.log(Level.INFO, "Spawn LFD Thread");
            while(!Thread.currentThread().isInterrupted()) {
                pinGFD();
            }
        });
        gfd.start();
    }

    private void pinGFD () {
        int heartbeatCount = 0;
        while (true) {
            heartbeatCount += 1;

            String message = "<RM" + ", " + "GFD" + ", heartbeat " + heartbeatCount + ", request>";
            byte[] payloadByte = message.getBytes();

            try (AsynchronousSocketChannel client = AsynchronousSocketChannel.open()) {
                Future<Void> result = client.connect(new InetSocketAddress(gfdAddress, gfdPort));
                result.get();
                ByteBuffer buffer = ByteBuffer.wrap(payloadByte);
                Future<Integer> writeval = client.write(buffer);

                logger.log(Level.INFO, "Sent " + message, true);

                long startTime = System.nanoTime();  

                // end sending portion, wait for response

                writeval.get();
                buffer = ByteBuffer.allocate(1024);

                // receive response
                Future<Integer> readval = client.read(buffer);
                int timeoutcount = 0;
                boolean isTimeOut = false;
                while (!readval.isDone() && !readval.isCancelled()) {
                    timeoutcount += 1;
                    if (timeoutcount == timeout){
                        isTimeOut = true;
                        break;
                    }
                    this.sleep(500);
                }

                double estimatedTime = (System.nanoTime() - startTime) / 1000000;
                updateMetric(estimatedTime);

                if (isTimeOut) {
                    // run timeout process
                    timeoutProcess();

                }else{
                    String response = new String(buffer.array()).trim();
                    logger.log(Level.INFO, "Received " + response);

                    readval.get();
                    buffer.clear();
                }
                this.sleep(this.heartbeatInterval);
            } catch (ExecutionException e) {
                timeoutProcess();
                try {
                    this.sleep(this.heartbeatInterval);
                }catch (Exception ethread) {
                        logger.log(Level.SEVERE, ethread.getMessage());
                }
                pinGFD();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void timeoutProcess(){
        logger.log(Level.WARNING, "Failed Heartbeat");
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

    private void updateMetric (double elapsedTime) {
        if (elapsedTime > this.maxLatencyGFD) {
            this.maxLatencyGFD = elapsedTime;
        }
        if (elapsedTime < this.minLatencyGFD) {
            this.minLatencyGFD = elapsedTime;
        }
        avgLatencyGFD = (avgLatencyGFD * this.messageCount + elapsedTime) / (this.messageCount + 1);
        this.messageCount += 1; 

        logger.log(Level.INFO, "elapsed time: " + elapsedTime, "Member");
    }

    private void spawnLogThread(){
        Thread logThread = new Thread(() -> {
            logger.log(Level.INFO, "Spawn Log Thread");
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    writeLog();
                    maxLatencyGFD = 0;
                    minLatencyGFD = Double.POSITIVE_INFINITY;
                    sleep(this.logInterval);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage());
                }
            }
        });
        logThread.start();
    }

    public void writeLog() throws IOException {
        if (this.minLatencyGFD == Double.POSITIVE_INFINITY) {
            return;
        }

        Date date = new Date();
        Timestamp timestamp = new Timestamp(date.getTime());
        String currentTime = sdf3.format(timestamp);

        File logFile = new File(fileName);
        logFile.getParentFile().mkdirs();
        logFile.createNewFile(); 

        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true));
        writer.append(currentTime + "," + this.minLatencyGFD + "," + this.maxLatencyGFD + "," + this.avgLatencyGFD);
        writer.append("\n");
        
        writer.close();
    }
}
