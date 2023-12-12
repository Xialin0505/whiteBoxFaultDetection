import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Server extends Thread{

    private String serverID = "S1";
    private String serverAddress = "127.0.0.1";
    private int serverPort = 8080;
    private boolean run;
    private Log logger = new Log(this.getClass().getName());
    private Thread lfdThread;
    private Thread inputThread;
    private InetSocketAddress serverSocket;
    private Map<String, InetSocketAddress> backupInfo;
    private List<String> backupID;
    private AsynchronousServerSocketChannel asyncServer;
    public int serverStatus;
    private boolean sendCheckpoint;
    AsynchronousSocketChannel client;
    private Thread mainThread;
    private Thread checkpointThread;
    private int iAmReady;
    private List<Integer> high_watermark_request_num = new ArrayList<Integer>();

    private final int LFDTimeout = 10;
    private int LFDwait = 0;

    private int checkpointCount;
    private int checkpointFrequency;
    private boolean isPrimary;
    private int backupNumber;

    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    private static final SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void startServer()
    {
        serverSocket = new InetSocketAddress(serverAddress, serverPort);
        this.run = true;
        this.start();
    }

    public void stopServer() throws IOException {
        run = false;
        this.interrupt();
        if (this.client != null){
            client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server();
        server.serverID = args[0];
        server.serverAddress = args[1];
        server.serverPort = Integer.parseInt(args[2]);

        server.backupNumber = 0;
        server.backupInfo = new HashMap<String, InetSocketAddress>();
        server.backupID = new ArrayList<String>();
        server.backupID.add(server.serverID);

        server.checkpointCount = 1;
        server.isPrimary = true;
        server.checkpointFrequency = 5000;
        server.sendCheckpoint = false;

        server.iAmReady = 0;

        server.high_watermark_request_num.add(0, 0);    // c1
        server.high_watermark_request_num.add(1, 0);    // c2
        server.high_watermark_request_num.add(2, 0);    // c3


        server.startServer();
    }

    @Override
    public void run() {
        mainThread = Thread.currentThread();
        startInputThread();
        startLFDThread();

        try (AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open()) {
            logger.log(Level.INFO, "Server " + serverID + " Running On IP " + serverAddress + " Port " + serverPort);
            server.bind(serverSocket);
            this.asyncServer = server;
            serverRunning();
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    private void serverRunning() throws IOException {
        try {
            while (this.run) {
                Future<AsynchronousSocketChannel> acceptCon = this.asyncServer.accept();
                client = acceptCon.get(20, TimeUnit.SECONDS);
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
                    String str = handleMessage(message);
                    if (str.length() > 0 ){
                        Future<Integer> writeVal = client.write(ByteBuffer.wrap(str.getBytes()));
                        writeVal.get();
                        buffer.clear();
                    }
                }

            }
        } catch (InterruptedException e) {
            logger.log(Level.INFO, "Server terminate");
            if (client != null){
                client.close();
            }
        } catch (TimeoutException e) {
            logger.log(Level.WARNING, e.getMessage());
            logger.log(Level.WARNING, "Timeout... Ending server socket");
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

    private String handleMessage(String message) {
        String[] messageType = message.split(", ");
        String str ="";
        if (messageType[2].contains("heartbeat")) {
            logger.log(Level.INFO, "Received " + message, true);
            str = processHeartbeatMessage(messageType);
            this.LFDwait = 0;
        } else if (messageType[2].contains("checkpoint")) {
            logger.log(Level.INFO, "Received " + message, "Checkpoint");
            processCheckpointMessageByBackup(messageType);
        } else if (messageType[2].contains("backup")) {
            logger.log(Level.INFO, "Received " + message, "Backup");
            if (messageType[2].contains("backup add")) {
                String newServerID = registerBackup(messageType);
                logger.log(Level.INFO, "Sending checkpoint");
                sendCheckpointMessageByPrimary(newServerID);
            } else if (messageType[2].contains("backup delete")) {
                deleteBackup(messageType);
            } else if (messageType[2].contains("backup membership")) {
                messageType[3] = messageType[3].replace(">", "");
                String[] membershipInfo = messageType[3].split(" ");

                for (int i = 0; i < membershipInfo.length; i++) {
                    logger.log(Level.WARNING, membershipInfo[i]);
                    this.backupID.add(membershipInfo[i]);
                }

                this.backupNumber = this.backupID.size();
                logger.log(Level.INFO, " having " + this.backupNumber + " replica: " + printMember(), "Member");
            }
        } else {
            try {
                logger.log(Level.INFO, "Received " + message);
                str = "";

                if (iAmReady == 1){
                    logger.log(Level.INFO,
                    "my_state_" + serverID + " = " + this.serverStatus + " before processing " + message, "State");
                    str = processClientMessage(message);
                    logger.log(Level.INFO,
                        "my_state_" + this.serverID + " = " + this.serverStatus + " after processing " + message, "State");

                    logger.log(Level.INFO, "Sending " + str);
                }
                else if (iAmReady == 0) {
                    // ? record the req num when the server got killed and recovering and waiting for checkpoint
                    updateHighWatermarkReqNum(message);
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage());
            }

        }
        return str;
    }

    private void startInputThread() {
        inputThread = new Thread(() -> {
            logger.log(Level.INFO, "Starting input thread...");
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    System.out.println("Server Command: ");
                    Scanner scanner = new Scanner(System.in);
                    runCommand(scanner.nextLine());
                } catch(Exception e) {
                    logger.log(Level.SEVERE, e.getMessage());
                }
            }
            logger.log(Level.INFO, "Closing input thread...");
        });
        inputThread.start();
    }

    private void startLFDThread() {
        lfdThread = new Thread(() -> {
            logger.log(Level.INFO, "Starting LFD thread...");
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    this.LFDwait += 1;
                    Thread.sleep(1000);
                    if (this.LFDwait > this.LFDTimeout) {
                        logger.log(Level.SEVERE, "Did not detect LFD, server terminate");
                        mainThread.interrupt();
                        System.exit(0);
                    }
                } catch(Exception e) {
                    logger.log(Level.SEVERE, e.getMessage());
                }
            }
            logger.log(Level.INFO, "Closing LFD thread...");
        });
        lfdThread.start();
    }

    private void runCommand(String command) throws IOException {
        if (command.toLowerCase().equals("pause")) {
            this.run = false;
            logger.log(Level.INFO, "Pausing server...");
        } else if (command.toLowerCase().equals("stop")) {
            logger.log(Level.INFO, "Stopping server...");
            this.stopServer();
        } else if (command.toLowerCase().equals("resume")) {
            logger.log(Level.INFO, "Resuming server...");
            this.run = true;
            run();
        } else {
            try{
                if (this.isPrimary){
                    this.checkpointFrequency = Integer.parseInt(command);
                    logger.log(Level.INFO, "Changing checkpointing frequency to " + command + " ms interval.");
                    this.sendCheckpoint = true;
                }
            }catch (NumberFormatException ex) {
                // do nothing;
                logger.log(Level.WARNING, "Invalid frequency " + ex.getMessage());
            }
        }
    }

    private String registerBackup(String[] messageType){
        
        messageType[4] = messageType[4].replace(">", "");
        String[] thisbackup = messageType[4].split(":");
        String thisbackupID = messageType[3].replace(" ", "");
        
        this.backupInfo.put(thisbackupID, new InetSocketAddress(thisbackup[0], Integer.parseInt(thisbackup[1])));
        
        this.backupID.add(thisbackupID);

        this.backupNumber = this.backupID.size();

        logger.log(Level.INFO, " having " + this.backupNumber + " replicas: " + printMember(), "Member");
        return thisbackupID;
    }

    private void updateHighWatermarkReqNum(String message) {
        String[] messageType = message.split(", ");

        int request_num = Integer.parseInt(messageType[2].trim());  // get req num from message

        // update high watermark request number
        if (messageType[0].contains("C1")) {
            high_watermark_request_num.set(0, request_num);
        }
        else if (messageType[0].contains("C2")) {
            high_watermark_request_num.set(1, request_num);
        }
        else if (messageType[0].contains("C3")) {
            high_watermark_request_num.set(2, request_num);
        }
        else {      // client not C1, C2, C3 error
            logger.log(Level.SEVERE, "Get Request Number Error!");
        }
        logger.log(Level.INFO, "Updated High Watermark Request Number: " + Arrays.toString(high_watermark_request_num.toArray()));
    }

    private String processClientMessage(String message) {
        changeServerStatus();

        String str = "";
        if (message.length() > 8) {
            str = message.substring(0, message.length() - 8) + "reply>";
        }
        logger.log(Level.INFO, "my_state_" + this.serverID + " = " + this.serverStatus + " during processing " + message);
        return str;
    }

    private void changeServerStatus(){
        this.serverStatus += 1;
    }

    private String processHeartbeatMessage(String[] messageType){
        System.out.println(Arrays.toString(messageType));
        Pattern pattern = Pattern.compile("action\\s*=\\s*\\[(.*?)\\]");
        String action = "";
        for (String message : messageType) {
            Matcher matcher = pattern.matcher(message);
            if (matcher.find()) {
                action = matcher.group(1);
                break;
            }
        }
        if (action.equals("INITIALIZATION")) {
            iAmReady = 1;
        } else if (action.equals("WAIT")) {
            iAmReady = 0;
        }
        String str = messageType[0] + "," + messageType[1] + ", " + this.serverStatus + ", reply>";
        return str;
    }

    private void processCheckpointMessageByBackup(String[] message) {
        if (iAmReady == 0){
            this.serverStatus = Integer.parseInt(message[3]);
            message[4] = message[4].replace(">", "");
            this.checkpointCount = Integer.parseInt(message[4]);
            iAmReady = 1;
            logger.log(Level.INFO, serverID + " server status " + this.serverStatus + " check point #" + this.checkpointCount, "Checkpoint");
        } else {
            logger.log(Level.INFO, serverID + " received duplicate checkpoint");
        }
    }

    private String printMember() {
        String loggerinfo = "";
        for (int i = 0; i < this.backupID.size(); i++) {
            loggerinfo = loggerinfo + " " + this.backupID.get(i);
        }
        return loggerinfo;
    }

    private void deleteBackup(String[] messageType) {
        String serverID = messageType[3].replace(">", "");
        backupInfo.remove(serverID);
        this.backupID.remove(this.backupID.indexOf(serverID));
        
        this.backupNumber = this.backupID.size();
        logger.log(Level.INFO, " having " + this.backupNumber + " replicas: " + printMember(), "Member");
    }

    private void sendCheckpointMessageByPrimary(String newServerID) {
        if (iAmReady == 0) {
            return;
        }

        String payload = "<" + serverID + ", " + newServerID + ", checkpoint, " + this.serverStatus + ", " +  this.checkpointCount + ">"; // primary -> backup: <primaryID, backupID, "checkpoint", serverStatus, checkpointCount>
        byte[] payloadByte = serverInputToByteArr(payload);

        try (AsynchronousSocketChannel server = AsynchronousSocketChannel.open()) {

            Future<Void> result = server.connect(this.backupInfo.get(newServerID));

            logger.log(Level.INFO, "Server " + serverID + " Connect to Backup " + newServerID, "Checkpoint");

            result.get(1, TimeUnit.SECONDS);
            ByteBuffer buffer = ByteBuffer.wrap(payloadByte);
            Future<Integer> writeval = server.write(buffer);

            logger.log(Level.INFO, "Sent " + payload, "Checkpoint");

            writeval.get();

        } catch (ExecutionException e) {
            logger.log(Level.WARNING, "Connection refused, Backup " + newServerID + " down/busy.");
        } catch (InterruptedException e) {
            logger.log(Level.WARNING, "Disconnected from Backup " + newServerID + ".");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Connection refused, Backup " + newServerID + " down/busy.");
            logger.log(Level.SEVERE, e.getMessage());
        }


        logger.log(Level.INFO, serverID + " server status " + this.serverStatus + " check point #" + this.checkpointCount, "Checkpoint");
        this.checkpointCount ++;
    }

    private byte[] serverInputToByteArr(String payload) {
        return payload.getBytes();
    }
}
