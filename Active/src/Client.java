import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.net.*;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class Client extends Thread {
    private static final SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Log logger = new Log(this.getClass().getName());

    private String clientID;
    private String[] serverIDs;
    private int serverNum;
    private String[] serverAddresses;
    private int[] serverPorts;
    private int requestNum;

    private double[] maxLatencyServer = {0, 0, 0};
    private double[] minLatencyServer = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    private double[] avgLatencyServer = {0, 0, 0};
    private int[] messageCount = {0, 0, 0};
    private String[] fileName = {"", "", ""};

    private int logInterval = 10000;

    public static void main(String[] args) {
        // 1st argument is the client name: C1/C2/C3
        // 2nd argument is the primary server name: S1
        // 3rd argument is the primary server address
        // 4th argument is the primary server port
        Client client = new Client();
        client.clientID = args[0];
        client.serverIDs = args[1].split(",");
        client.serverNum = client.serverIDs.length;
        client.serverAddresses = args[2].split(",");
        String[] ports = args[3].split(",");
        client.serverPorts = new int[ports.length];
        for (int i = 0; i < client.serverNum; i++) {
            client.serverPorts[i] = Integer.parseInt(ports[i]);
            client.fileName[i] = "../log/" + client.clientID + client.serverIDs[i] + ".csv";
            client.spawnLogThread(i);
        }
        
        client.requestNum = 0;
        client.runClient();
    }

    private void runClient() {

        while (true) {
            boolean[] isDown = new boolean[3];
            requestNum++;
            int prevRequestNum = -1;

            for (int i = 0; i < serverNum; i++) {
                String serverID = serverIDs[i];
                String serverAddress = serverAddresses[i];
                int serverPort = serverPorts[i];

                try (AsynchronousSocketChannel client = AsynchronousSocketChannel.open()) {

                    String payload = "<" + clientID + ", " + serverID + ", " + requestNum + ", Hello, request>";
                    byte[] payloadByte = clientInputToByteArr(payload);

                    Future<Void> result = client.connect(new InetSocketAddress(serverAddress, serverPort));

                    logger.log(Level.INFO, "Client " + clientID + " Connect to Server " + serverID + " IP " + serverAddress + " Port " + serverPort);

                    result.get(20, TimeUnit.SECONDS);
                    ByteBuffer buffer = ByteBuffer.wrap(payloadByte);
                    Future<Integer> writeval = client.write(buffer);

                    logger.log(Level.INFO, "Sent " + payload);
                    
                    long startTime = System.nanoTime();     
                    // end sending portion, wait for response

                    writeval.get();
                    buffer = ByteBuffer.allocate(1024);

                    // receive response
                    // int retry = 0;
                    boolean skip = false;
                    Future<Integer> readval = client.read(buffer);
                    while (!readval.isDone() && !readval.isCancelled()) {
                        // if (retry == 3){
                        //     skip = true;
                        //     logger.log(Level.INFO, "Client does not receive reply from " + serverID, "Client");
                        //     break;
                        // }
                        Thread.sleep(1);
                        // retry++;
                    }

                    double estimatedTime = (System.nanoTime() - startTime) / 1000000;
                    updateMetric(estimatedTime, i);

                    if (!skip){
                        String response = new String(buffer.array()).trim();

                        logger.log(Level.INFO, "Received " + response);

                        String tmp = response.split(" ")[2];
                        int tmpN = Integer.parseInt(tmp.substring(0, tmp.length() - 1));

                        if (tmpN == prevRequestNum) {
                            logger.log(Level.INFO, "request_num " + tmpN + ": Discarded duplicate reply from " + serverID + ".", "Client");
                        } else {
                            prevRequestNum = tmpN;
                        }

                        readval.get();
                        buffer.clear();
                        Thread.sleep(3000);
                    }

                } catch (ExecutionException e) {
                    logger.log(Level.WARNING, "Connection refused, server down/busy.");
                    isDown[i] = true;
                    try {
                        Thread.sleep(3000);
                    } catch (Exception esleep) {
                        logger.log(Level.SEVERE, esleep.getMessage());
                    }
                } catch (InterruptedException e) {
                    logger.log(Level.INFO, "Disconnected from the server.");
                    isDown[i] = true;
                } catch (Exception e) {
                    isDown[i] = true;
                    logger.log(Level.SEVERE, e.getMessage());
                }
            }
            try {
                Thread.sleep(3000);
            } catch (Exception esleep) {
                logger.log(Level.SEVERE, esleep.getMessage());
            }
        }
    }

    private void updateMetric (double elapsedTime, int server) {
        if (elapsedTime > this.maxLatencyServer[server]) {
            this.maxLatencyServer[server] = elapsedTime;
        }
        if (elapsedTime < this.minLatencyServer[server]) {
            this.minLatencyServer[server] = elapsedTime;
        }
        avgLatencyServer[server] = (avgLatencyServer[server] * this.messageCount[server] + elapsedTime) / (this.messageCount[server] + 1);
        this.messageCount[server] += 1; 

        logger.log(Level.INFO, "elapsed time: " + elapsedTime, "Member");
    }

    private String getClientManualInput (String output, String serverID) {
        System.out.println(output);
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String message = "";
        String line = ""; 
        
        try {
            while ((line = br.readLine()) != null && line.length()!= 0) { 
                message += line;
            } 
        } catch ( IOException e){
            logger.log(Level.SEVERE, e.getMessage());
        }

        String payload = "<" + clientID + ", " + serverID + ", " + requestNum + ", " + message + ", request>";

        return payload;
    }

    private byte[] clientInputToByteArr(String payload) {
        return payload.getBytes();
    }

    private String generateServerStatusInfo(boolean[] isDown) {
        StringBuilder info = new StringBuilder("Server Status: ");
        for (int i = 0; i < serverNum; i++) {
            String serverStatus = isDown[i] ? "Down" : "Up";
            info.append(serverIDs[i]).append(" is ").append(serverStatus);
            if (i < isDown.length - 1) {
                info.append(", ");
            }
        }
        return info.toString();
    }

    private void spawnLogThread(int server){
        Thread logThread = new Thread(() -> {
            logger.log(Level.INFO, "Spawn Log Thread");
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    writeLog(server);
                    maxLatencyServer[server] = 0;
                    minLatencyServer[server] = Double.POSITIVE_INFINITY;
                    sleep(this.logInterval);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage());
                }
            }
        });
        logThread.start();
    }

    public void writeLog(int server) throws IOException {
        if (this.minLatencyServer[server] == Double.POSITIVE_INFINITY) {
            return;
        }
        
        Date date = new Date();
        Timestamp timestamp = new Timestamp(date.getTime());
        String currentTime = sdf3.format(timestamp);

        File logFile = new File(fileName[server]);
        logFile.getParentFile().mkdirs();
        logFile.createNewFile(); 

        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName[server], true));
        writer.append(currentTime + "," + this.minLatencyServer[server] + "," + this.maxLatencyServer[server] + "," + this.avgLatencyServer[server]);
        writer.append("\n");
        
        writer.close();
    }

}