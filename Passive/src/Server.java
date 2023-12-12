import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class Server extends Thread{

    private String serverID = "S1";
    private String serverAddress = "127.0.0.1";
    private int serverPort = 8080;
    private boolean run;
    private Log logger = new Log(this.getClass().getName());
    private Thread lfdThread;
    private Thread inputThread;
    private InetSocketAddress serverSocket;
    private List<InetSocketAddress> backupSocket;
    private List<String> backupID;
    private AsynchronousServerSocketChannel asyncServer;
    public int serverStatus;
    private boolean sendCheckpoint;
    private Semaphore semaphore;
    AsynchronousSocketChannel client;
    private Thread mainThread;
    private Thread checkpointThread;

    private final int LFDTimeout = 10;
    private int LFDwait = 0;

    private int checkpointCount;
    private int checkpointFrequency;
    private boolean isPrimary;
    private int backupNumber;
    
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

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
        server.backupSocket = new ArrayList<InetSocketAddress>();
        server.backupID = new ArrayList<String>();

        server.checkpointCount = 1;
        server.isPrimary = false;
        server.semaphore = new Semaphore(1);
        server.checkpointFrequency = 5000;
        server.sendCheckpoint = false;

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
                    if (str.length() > 0){
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
        } else if (!this.isPrimary && messageType[2].contains("checkpoint")) {
            logger.log(Level.INFO, "Received " + message, "Checkpoint");
            processCheckpointMessageByBackup(messageType);
        } else if (messageType[2].contains("backup")) {
            logger.log(Level.INFO, "Received " + message, "Backup");
            this.isPrimary = true;
            logger.log(Level.INFO, this.serverID + " become primary", "Member");
            registerBackup(messageType);
            if (this.checkpointThread == null) {
                logger.log(Level.INFO, "Initial registration for checkpoint Thread");
                spawnCheckpointThread();
            }
        } else if (this.isPrimary){
            try {
                logger.log(Level.INFO, "Received " + message); 

                logger.log(Level.INFO,
                    "my_state_" + serverID + " = " + this.serverStatus + " before processing " + message, "State");
                str = processClientMessage(message);
                logger.log(Level.INFO,
                    "my_state_" + this.serverID + " = " + this.serverStatus + " after processing " + message, "State");
                
                logger.log(Level.INFO, "Sending " + str);
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

    private void spawnCheckpointThread() {
        this.checkpointThread = new Thread(() -> {
            while(!Thread.currentThread().isInterrupted()) {
                if ("Checkpoint".equals(Thread.currentThread().getName())){
                    try {
                        this.semaphore.acquire();
                        sendCheckpointMessageByPrimary();
                        this.semaphore.release();
                        checkpointThread.sleep(this.checkpointFrequency);
                    } catch (Exception e) {
                        logger.log(Level.WARNING, e.getMessage());
                    }
                }
                
            }
            logger.log(Level.INFO, "Closing checkpoing thread...");
        });
        
        this.checkpointThread.setName("Checkpoint");
        this.checkpointThread.start();
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

    private void registerBackup(String[] messageType){
        this.backupID = new ArrayList<String>();
        this.backupSocket = new ArrayList<InetSocketAddress>();
        this.backupNumber = 0;

        if (messageType[3].contains("empty")){
            logger.log(Level.INFO, "Number of backups: " + this.backupNumber, "Backup");
            return;
        }
        
        messageType[3] = messageType[3].replace(">", "");
        String[] backupInfo = messageType[3].split(" ");

        for (int i = 0; i < backupInfo.length; i++){
            String[] idAddrPort = backupInfo[i].split(":");
            backupID.add(idAddrPort[0]);
            backupSocket.add(new InetSocketAddress(idAddrPort[1], Integer.parseInt(idAddrPort[2])));
            this.backupNumber++;
        }

        logger.log(Level.INFO, "Number of backups: " + this.backupNumber, "Backup");
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
        String str = messageType[0] + "," + messageType[1] + ", " + this.serverStatus + ", reply>";
        return str;
    }

    private void processCheckpointMessageByBackup(String[] message) {
        this.serverStatus = Integer.parseInt(message[3]);
        message[4] = message[4].replace(">", "");
        this.checkpointCount = Integer.parseInt(message[4]);
        logger.log(Level.INFO, serverID + " server status " + this.serverStatus + " check point #" + this.checkpointCount, "Checkpoint");
    }

    private void sendCheckpointMessageByPrimary() {

        for (int i = 0; i < backupNumber; i++) {
            String payload = "<" + serverID + ", " + this.backupID.get(i) + ", checkpoint, " + this.serverStatus + ", " +  this.checkpointCount + ">"; // primary -> backup: <primaryID, backupID, "checkpoint", serverStatus, checkpointCount>
            byte[] payloadByte = serverInputToByteArr(payload);
            
            try (AsynchronousSocketChannel server = AsynchronousSocketChannel.open()) {

                Future<Void> result = server.connect(this.backupSocket.get(i));

                logger.log(Level.INFO, "Server " + serverID + " Connect to Backup " + this.backupID.get(i), "Checkpoint");

                result.get(1, TimeUnit.SECONDS);
                ByteBuffer buffer = ByteBuffer.wrap(payloadByte);
                Future<Integer> writeval = server.write(buffer);

                logger.log(Level.INFO, "Sent " + payload, "Checkpoint");

                writeval.get();
               

            } catch (ExecutionException e) {
                logger.log(Level.WARNING, "Connection refused, Backup " + backupID + " down/busy.");
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "Disconnected from Backup " + backupID + ".");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Connection refused, Backup " + backupID + " down/busy.");
                logger.log(Level.SEVERE, e.getMessage());
            }
        }

        this.checkpointCount ++;
    }

    private byte[] serverInputToByteArr(String payload) {
        return payload.getBytes();
    }
}
