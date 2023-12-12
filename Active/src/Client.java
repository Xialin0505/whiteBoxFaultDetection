import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.net.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class Client {
    private static final SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Log logger = new Log(this.getClass().getName());

    private String clientID;
    private String[] serverIDs;
    private int serverNum;
    private String[] serverAddresses;
    private int[] serverPorts;
    private int requestNum;

    // You can manually send messages from each client.

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
        for (int i = 0; i < client.serverNum; i++) client.serverPorts[i] = Integer.parseInt(ports[i]);
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

                    result.get(1, TimeUnit.SECONDS);
                    ByteBuffer buffer = ByteBuffer.wrap(payloadByte);
                    Future<Integer> writeval = client.write(buffer);

                    logger.log(Level.INFO, "Sent " + payload);

                    // end sending portion, wait for response

                    writeval.get();
                    buffer = ByteBuffer.allocate(1024);

                    // receive response
                    int retry = 0;
                    boolean skip = false;
                    Future<Integer> readval = client.read(buffer);
                    while (!readval.isDone() && !readval.isCancelled()) {
                        if (retry == 3){
                            skip = true;
                            logger.log(Level.INFO, "Client does not receive reply from " + serverID, "Client");
                            break;
                        }
                        Thread.sleep(1000);
                        retry++;
                    }

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

}