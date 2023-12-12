import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

public class LocalFaultDetector extends Thread{
    // timeout is 5s
    private static int timeout = 5;
    private Log logger = new Log(this.getClass().getName());

    // need another threads to listen to client input for change heartbeat freq
    private int heartbeatInterval;
    private boolean heartBeatUpdate = false;
    private int heartbeatCount = 1;
    private String lfdID;
    private int lfdPort;
    private String serverID;
    private String serverAddress;
    private int serverPort;
    private InetSocketAddress serverSocket;
    private InetSocketAddress lfdSocket;
    private String gfdAddress;
    private int gfdPort;
    private InetSocketAddress gfdSocket;
    private Thread inputThread;
    private boolean serverRegisteredToGfd = false;
    private Thread mainThread;
    private AsynchronousServerSocketChannel asyncServer;
    private AsynchronousSocketChannel serverClient;
    private int serverStatus; // 0 -> initial server run, 1 -> server running regularly, 2 -> server died and need recover

    public static void main (String[] args) {
        LocalFaultDetector lfd = new LocalFaultDetector();
        lfd.lfdID = args[0];
        lfd.lfdPort = Integer.parseInt(args[1]);
        lfd.heartbeatInterval = Integer.parseInt(args[2]);
        lfd.serverID = args[3];
        lfd.serverAddress = args[4];
        lfd.serverPort = Integer.parseInt(args[5]);
        lfd.gfdAddress = args[6];
        lfd.gfdPort = Integer.parseInt(args[7]);
        lfd.serverStatus = 0;
        lfd.runFaultDetector();
    }

    public void runFaultDetector() {
        logger.log(Level.INFO, "Server " + lfdID + " Connect to Server " + serverID + " IP " + serverAddress + " Port " + serverPort);
        this.serverSocket = new InetSocketAddress(serverAddress, serverPort);
        this.gfdSocket = new InetSocketAddress(gfdAddress, gfdPort);
        this.lfdSocket = new InetSocketAddress(serverAddress, lfdPort);
        this.start();
    }

    @Override
    public void run() {
        mainThread = Thread.currentThread();
        startInputThread();
        logger.log(Level.INFO, "Registering LFD to GFD ...");
        sendUpdateToGfd("register", serverAddress, lfdPort);
        spawnLFDThread();
        spawnLFDServer();
    }

    private void spawnLFDThread(){
        Thread lfd = new Thread(() -> {
            logger.log(Level.INFO, "Spawn LFD Thread");
            while(!Thread.currentThread().isInterrupted()) {
                startFaultDetector();
            }
        });
        lfd.start();
    }

    private void spawnLFDServer(){
        try (AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open()) {
            logger.log(Level.INFO, "Server " + lfdID + " Running On IP " + serverAddress + " Port " + lfdPort);
            server.bind(this.lfdSocket);
            this.asyncServer = server;
            serverRunning();
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    private void startFaultDetector() {
        // start register GFD using "fldID: add replica serverID lfdAddress lfdPort" format
        // while (true) {
            try (AsynchronousSocketChannel client = AsynchronousSocketChannel.open()) {
                heartbeatCount += 1;
                heartBeatToServer(client);
                this.sleep(this.heartbeatInterval);
            } catch (ExecutionException e) {
                timeoutProcess();
                try {
                    this.sleep(this.heartbeatInterval);
                }catch (Exception ethread) {
                    if (!heartBeatUpdate){
                        logger.log(Level.SEVERE, ethread.getMessage());
                    }
                }
                startFaultDetector();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage());
            } catch (InterruptedException e) {
                if (!heartBeatUpdate) {
                    e.printStackTrace();
                } else {
                    logger.log(Level.INFO, "Heartbeat interval updated");
                }
            }
        //}
    }

    private void heartBeatToServer(AsynchronousSocketChannel client) throws ExecutionException, InterruptedException {
        String message;
        Future<Void> serverResult = client.connect(serverSocket);
        serverResult.get();
        if (serverStatus == 0) {
            message = "<" + lfdID + ", " + serverID + ", heartbeat " + heartbeatCount + ", request, action = [INITIALIZATION] >";
        } else if (serverStatus == 1){
            message = "<" + lfdID + ", " + serverID + ", heartbeat " + heartbeatCount + ", request>";
        } else { // serverStatus  == 2
            message = "<" + lfdID + ", " + serverID + ", heartbeat " + heartbeatCount + ", request, action = [WAIT] >";
        }
        byte[] payloadByte = message.getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(payloadByte);
        Future<Integer> writeval = client.write(buffer);
        logger.log(Level.INFO, "Sent " + message, true);


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
            this.sleep(1000);
        }

        if (isTimeOut) {
            timeoutProcess();
        }else{
            String response = new String(buffer.array()).trim();
            logger.log(Level.INFO, "Received " + response);
            serverStatus = 1;
            try {
                if (!serverRegisteredToGfd) {
                    sendUpdateToGfd("add", serverAddress, lfdPort);
                    serverRegisteredToGfd = true;
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, e.getMessage());
            }
            readval.get();
            buffer.clear();
        }
    }

    private void sendUpdateToGfd(String gfdAction, String address, int port) {
        int retry = 3;
        int count = 0;
        while (true) {
            try (AsynchronousSocketChannel gfdClient = AsynchronousSocketChannel.open()) {
                Future<Void> gfdResult = gfdClient.connect(gfdSocket);

                gfdResult.get(1000, TimeUnit.MILLISECONDS);
                String gfdMessage;
                if (gfdAction.equals("register")) {
                    gfdMessage = "<" + lfdID + ": " + gfdAction + " " + serverID + " " + address + ":" + port + ">";
                } else {
                    gfdMessage = "<" + lfdID + ": " + gfdAction + " replica " + serverID + " " + address + ":" + port + ">";
                }
                ByteBuffer gfdBuffer = ByteBuffer.wrap(gfdMessage.getBytes());
                Future<Integer> gfdWriteVal = gfdClient.write(gfdBuffer);
                gfdWriteVal.get();
                logger.log(Level.INFO, "Sent " + gfdMessage + " to GFD", true);
                break;
            } catch (Exception e) {
                count++;
                logger.log(Level.WARNING, "Error sending message to GFD, retrying...");
                if (count == retry) {
                    logger.log(Level.SEVERE, "Error sending message to GFD, closing LFD...");
                    mainThread.interrupt();
                    System.exit(0);
                }
                try {
                    this.sleep(1000);
                } catch (Exception sleepExp) {
                    
                }
            }
        }

    }

    private void serverRunning() throws IOException{
        try {
            while (true) {
                Future<AsynchronousSocketChannel> acceptCon = this.asyncServer.accept();
                serverClient = acceptCon.get(10, TimeUnit.SECONDS);

                if ((serverClient != null) && (serverClient.isOpen())) {
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    Future<Integer> readval = serverClient.read(buffer);
                    while (!readval.isDone() && !readval.isCancelled()) {
                        Thread.sleep(1000);
                    }
                    String message = new String(buffer.array()).trim();

                    readval.get();
                    buffer.flip();
                    String str = "";
                    // process things
                    String[] messageType = message.split(", ");

                    if (messageType[2].contains("heartbeat")) {
                        logger.log(Level.INFO, "Received " + message, true);
                        str = processHeartbeatMessage(messageType);
                    } else if (messageType[2].contains("backup")) {
                        logger.log(Level.INFO, "Received " + message, "Backup");
                        forwardBackupMsg(messageType);
                        continue;
                    } else if (messageType[2].contains("checkpoint")) {
                        logger.log(Level.INFO, "Received " + message, "Checkpoint");
                        forwardCheckpointMsg(message);
                        continue;
                    }
                    
                    Future<Integer> writeVal = serverClient.write(ByteBuffer.wrap(str.getBytes()));
                    writeVal.get();
                    buffer.clear();
                }
            }
        } catch (InterruptedException e) {
            logger.log(Level.INFO, "Change heartbeat frequency");
        } catch (TimeoutException e) {
            logger.log(Level.WARNING, "Timeout... Ending server socket");
            if (serverClient != null){
                serverClient.close();
            }
            System.exit(0);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    private void timeoutProcess(){
        // run timeout process
        // if (serverStatus == 0) it's still not initialized, we do nothing
        if (serverStatus == 1) {
            serverStatus = 2; // server failed connection
        }
        try {
            if (serverRegisteredToGfd) {
                sendUpdateToGfd("delete", serverAddress, lfdPort);
                serverRegisteredToGfd = false;
                String classLocation = LocalFaultDetector.class.getResource("LocalFaultDetector.class").toString();
                String updatedLocation = classLocation.substring(0,classLocation.lastIndexOf("/"));
                updatedLocation = updatedLocation.substring(updatedLocation.indexOf("/")+1,updatedLocation.lastIndexOf("/"));
                updatedLocation = updatedLocation.substring(updatedLocation.indexOf("/")+1);
                updatedLocation = updatedLocation.substring(updatedLocation.indexOf("/")+1);
                System.out.println(updatedLocation);
                String commandToRun = "sh runServer" + serverID.substring(1) +".sh";
                // AppleScript command to open Terminal, navigate to the directory and run the command
                String appleScriptCommand = String.format(
                        "tell application \"Terminal\" to do script \"cd %s && %s\"",
                        updatedLocation, commandToRun
                );
                try {
                    System.out.println("Starting new server now...");
                    new ProcessBuilder("osascript", "-e", appleScriptCommand).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e){
            logger.log(Level.WARNING, e.getMessage());
        }
        logger.log(Level.WARNING, "Failed Heartbeat");
    }

    private String processHeartbeatMessage(String[] messageType){
        String str = messageType[0] + "," + messageType[1] + ", reply>";
        return str;
    }

    private void changeHeartbeat(String command) {
        this.heartbeatInterval = Integer.parseInt(command);
        this.heartBeatUpdate = true;
        logger.log(Level.INFO, "Changing LFD heartbeat frequency to " + command + " ms interval.");
        this.interrupt();
        this.start();
    }

    private void forwardBackupMsg(String[] messageType) {
        int retry = 3;
        int count = 0;
        while (true) {
            String backupMessage = "<" + this.lfdID + ", " + this.serverID;

            for (int i = 2; i < messageType.length; i++) {
                backupMessage = backupMessage + ", " + messageType[i];
            }

            ByteBuffer serverBuffer = ByteBuffer.wrap(backupMessage.getBytes());
            
            try (AsynchronousSocketChannel gfdClient = AsynchronousSocketChannel.open()) {
                Future<Void> serverResult = gfdClient.connect(serverSocket);
                serverResult.get(1000, TimeUnit.MILLISECONDS);

                Future<Integer> serverWriteVal = gfdClient.write(serverBuffer);
                serverWriteVal.get();
                logger.log(Level.INFO, "Sent " + backupMessage + " to Primary", "Backup");
                break;
            } catch (Exception e) {
                count++;
                logger.log(Level.WARNING, "Error sending message to server, retrying...");
                if (count == retry) {
                    try {
                        if (serverRegisteredToGfd){
                            logger.log(Level.WARNING, "Error sending message to server, update GFD...");
                            sendUpdateToGfd("delete", serverAddress, lfdPort);
                            serverRegisteredToGfd = false;
                        }
                    } catch (Exception se) {
                        logger.log(Level.WARNING, se.getMessage());
                    }
                    break;
                }
                try {
                    this.sleep(1000);
                } catch (Exception sleepExp) {

                }
            }
        }
    }

    private void forwardCheckpointMsg(String message) {
        int retry = 3;
        int count = 0;
        while (true) {
            
            ByteBuffer serverBuffer = ByteBuffer.wrap(message.getBytes());
            
            try (AsynchronousSocketChannel gfdClient = AsynchronousSocketChannel.open()) {
                Future<Void> serverResult = gfdClient.connect(serverSocket);
                serverResult.get(1000, TimeUnit.MILLISECONDS);

                Future<Integer> serverWriteVal = gfdClient.write(serverBuffer);
                serverWriteVal.get();
                logger.log(Level.INFO, "Sent " + message + " to server", "Checkpoint");
                break;
            } catch (Exception e) {
                count++;
                logger.log(Level.WARNING, "Error sending message to server, retrying...");
                if (count == retry) {
                    try {
                        if (serverRegisteredToGfd){
                            logger.log(Level.WARNING, "Error sending message to server, update GFD...");
                            sendUpdateToGfd("delete", serverAddress, lfdPort);
                            serverRegisteredToGfd = false;
                        }
                    } catch (Exception se) {
                        logger.log(Level.WARNING, se.getMessage());
                    }
                    break;
                }
                try { 
                    this.sleep(1000);
                } catch (Exception sleepExp) {

                }
            }
        }
    }

    private void startInputThread() {
        inputThread = new Thread(() -> {
            logger.log(Level.INFO, "Starting input thread...");
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    Scanner scanner = new Scanner(System.in);
                    changeHeartbeat(scanner.nextLine());
                } catch(Exception e) {
                    if (!heartBeatUpdate) {
                        logger.log(Level.SEVERE, e.getMessage());
                    }
                }
            }
            logger.log(Level.INFO, "Closing input thread...");
        });
        inputThread.start();
        heartBeatUpdate = false;
    }
    
    private void sendWaitToServer(){
        try (AsynchronousSocketChannel waitClient = AsynchronousSocketChannel.open()) {
            Future<Void> waitResult = waitClient.connect(serverSocket);

            waitResult.get(1000, TimeUnit.MILLISECONDS);
            String waitMessage = "<" + lfdID + ", " + serverID + ", wait>";
            ByteBuffer waitBuffer = ByteBuffer.wrap(waitMessage.getBytes());
            Future<Integer> gfdWriteVal = waitClient.write(waitBuffer);
            gfdWriteVal.get();
            logger.log(Level.INFO, "Sent " + waitMessage + " to Server", "Member");
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }

    }

}
