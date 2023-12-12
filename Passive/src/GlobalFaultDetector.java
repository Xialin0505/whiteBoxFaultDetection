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

public class GlobalFaultDetector extends Thread{
    private static final SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // timeout is 5s
    private static int timeout = 5;
    private Log logger = new Log(this.getClass().getName());

    private int heartbeatInterval;
    private boolean heartBeatUpdate = false;
    private String gfdID;
    private Thread inputThread;
    private String serverAddress;
    private int serverPort;
    private AsynchronousSocketChannel client;
    private InetSocketAddress serverSocket;
    private AsynchronousServerSocketChannel asyncServer;
    private List<String> membership; // server id
    private HashMap <String, Thread> lfds; // lfd id
    private HashMap <String, String> serverInfo;
    private int memberCount = 0;
    private String primaryAddress;
    private int primaryPort;
    private String primaryServerID;
    private String primaryLFD;
    private String RMIP;
    private int RMPort;
    private String RMID = "RM";

    public static void main (String[] args) {
        GlobalFaultDetector gfd = new GlobalFaultDetector();
        gfd.heartbeatInterval = Integer.parseInt(args[0]);
        gfd.gfdID = args[1];
        gfd.serverAddress = args[2];
        gfd.serverPort = Integer.parseInt(args[3]);
        gfd.RMIP = args[4];
        gfd.RMPort = Integer.parseInt(args[5]);
        gfd.lfds = new HashMap<String, Thread>();
        gfd.membership = new ArrayList<String>();
        gfd.serverInfo = new HashMap<String, String>();
        gfd.run();
    }

    @Override
    public void run() {
        startInputThread();
        this.serverSocket = new InetSocketAddress(serverAddress, serverPort);
        startFaultDetector();
    }

    private void startFaultDetector() {
        try (AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open()) {
            logger.log(Level.INFO, "GFD Running On IP " + serverAddress + " Port " + serverPort);
            printMember();
            updateRM();
            server.bind(this.serverSocket);
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
                client = acceptCon.get();
                
                if ((client != null) && (client.isOpen())) {
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    Future<Integer> readval = client.read(buffer);
                    while (!readval.isDone() && !readval.isCancelled()) {
                        Thread.sleep(1000);
                    }
                    String message = new String(buffer.array()).trim();

                    readval.get();
                    buffer.flip();
                    String str = "";
                    // process things
                    String[] messageType = message.split(" ");
                    System.out.println(message);

                    if (messageType[1].equals("delete")) {
                        logger.log(Level.INFO, "Received " + message, "Member");
                        diregisterMember(messageType);
                    }else if (messageType[1].equals("add")){
                        logger.log(Level.INFO, "Received " + message, "Member");
                        addMember(messageType);
                    } else if (messageType[1].equals("register")){
                        registerMember(messageType);
                    }

                    Future<Integer> writeVal = client.write(ByteBuffer.wrap(str.getBytes()));
                    writeVal.get();
                    buffer.clear();
                }
            }
        } catch (InterruptedException e) {
            logger.log(Level.INFO, "Server terminate");
            if (client != null){
                client.close();
            }
            System.exit(0);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }

    }

    private void registerMember(String[] input){
        String lfdID = input[0].substring(1, input[0].length()-1);
        String[] addAndPort = input[3].split(":");
        String lfdaddress = addAndPort[0];
        int lfdport = Integer.parseInt(addAndPort[1].substring(0, addAndPort[1].length()-1));
        spawnPinThread(lfdID, lfdaddress, lfdport);
    }

    private void addMember(String[] input){
        String serverID = input[3];
        
        int i = 0;
        for (; i < memberCount; i++){
            if (membership.get(i).equals(serverID)) {
                printMember();
                updateRM();
                return;
            }
        }

        this.serverInfo.put(serverID, input[4].substring(0, input[4].length()-1));
        membership.add(serverID);

        if (memberCount == 0) {
            String[] addAndPort = this.serverInfo.get(serverID).split(":");
            this.primaryAddress = addAndPort[0];
            this.primaryPort = Integer.parseInt(addAndPort[1]);
            this.primaryServerID = serverID;
            this.primaryLFD = "lfd" + serverID.substring(1);
        }

        memberCount ++;
        printMember();
        updateRM();
        setupPrimary();

    }

    private void spawnPinThread(String lfdID, String lfdadress, int lfdport){
        Thread lfdPin = new Thread(() -> {
            logger.log(Level.INFO, "Start Pin " + lfdID);
            while(!Thread.currentThread().isInterrupted()) {
                pinLFD(lfdID, lfdadress, lfdport);
            }
            logger.log(Level.INFO, "Stop Pin " + lfdID);
        });
        lfdPin.start();
        lfds.put(lfdID, lfdPin);
    }

    private void diregisterMember(String[] input){
        String serverID = input[3];
        for (int i = 0; i < this.memberCount; i++){
            if (this.membership.get(i).equals(serverID)) {
                this.membership.remove(i);
                this.serverInfo.remove(serverID);
                this.memberCount --;
                break;
            }
        }
        printMember();
        updateRM();

        // we assume that membership can not be zero
        if (this.memberCount > 0){
            serverID = this.membership.get(0);
            String[] addAndPort = this.serverInfo.get(serverID).split(":");
            this.primaryAddress = addAndPort[0];
            this.primaryPort = Integer.parseInt(addAndPort[1]);
            this.primaryServerID = serverID;
            this.primaryLFD = "lfd" + serverID.substring(1);
        }
        setupPrimary();
    }

    private void pinLFD (String lfdID, String lfdAddress, int lfdPort) {
        int heartbeatCount = 0;
        while (true) {
            heartbeatCount += 1;
                    
            String message = "<" + gfdID + ", " + lfdID + ", heartbeat " + heartbeatCount + ", request>";
            byte[] payloadByte = message.getBytes();

            try (AsynchronousSocketChannel client = AsynchronousSocketChannel.open()) {
                    Future<Void> result = client.connect(new InetSocketAddress(lfdAddress, lfdPort));
                    result.get();
                    ByteBuffer buffer = ByteBuffer.wrap(payloadByte);
                    Future<Integer> writeval = client.write(buffer);

                    logger.log(Level.INFO, "Sent " + message, true);

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
                        if (!heartBeatUpdate){
                            logger.log(Level.SEVERE, ethread.getMessage());
                        }
                    }
                    pinLFD(lfdID, lfdAddress, lfdPort);
                } catch (IOException e) {
                    logger.log(Level.SEVERE, e.getMessage());
                } catch (InterruptedException e) {
                    if (!heartBeatUpdate) {
                        e.printStackTrace();
                    } else {
                        logger.log(Level.INFO, "Heartbeat interval updated");
                    }
                }
        }
    }

    private byte[] serverInputToByteArr(String payload) {
        return payload.getBytes();
    }

    private void setupPrimary() {

        String backupInfo = "";
        
        for (int i = 1; i < memberCount; i++){
            String serverID = this.membership.get(i);
            backupInfo  = backupInfo + serverID + ":" + this.serverInfo.get(serverID);
            if (i < memberCount-1) {
                backupInfo += " ";
            } 
        }

        backupInfo = backupInfo.length() == 0? "empty" : backupInfo;

        int retry = 3;
        int count = 0;
        
        while (true) {

            try (AsynchronousSocketChannel server = AsynchronousSocketChannel.open()) {

                String payload = "<" + this.gfdID + ", " + this.primaryLFD + ", backup, " + backupInfo + ">"; // gfd -> lfd: <gfdID, primaryLFD , "backupInfo", backupInfo>
                byte[] payloadByte = serverInputToByteArr(payload);

                Future<Void> result = server.connect(new InetSocketAddress(primaryAddress, primaryPort));

                logger.log(Level.INFO, "GFD " + " Connect to LFD " + this.primaryLFD + " IP " + this.primaryAddress + " Port " + this.primaryPort, "Backup");

                result.get(1, TimeUnit.SECONDS);
                ByteBuffer buffer = ByteBuffer.wrap(payloadByte);
                Future<Integer> writeval = server.write(buffer);
                writeval.get();

                logger.log(Level.INFO, "Sent " + payload, "Backup");
                break;

            } catch (ExecutionException e) {
                count++;
                logger.log(Level.WARNING, "Connection refused, LFD " + this.primaryLFD + " down/busy.");

                if (count == retry) {
                    logger.log(Level.SEVERE, "Error sending message to LFD...");
                    break;
                }

                try {
                    Thread.sleep(3000);
                } catch (Exception esleep) {
                    logger.log(Level.SEVERE, esleep.getMessage());
                    break;
                }
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "Disconnected from LFD " + this.primaryLFD + ".");
                break;
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage());
                break;
            }
        }
    }

    private void timeoutProcess(){
        logger.log(Level.WARNING, "Failed Heartbeat");
    }

    private void changeHeartbeat(String command) {
        this.heartbeatInterval = Integer.parseInt(command);
        this.heartBeatUpdate = true;
        logger.log(Level.INFO, "Changing LFD heartbeat frequency to " + command + " ms interval.");
        this.interrupt();
        this.start();
    }

    private void printMember(){
        String str = "GFD Having " + memberCount + " Members:";
        str += getMember();
        logger.log(Level.INFO, str, "Member"); 
    }

    private String getMember() {
        String str = "";
        for (String e : membership){
            String serverID = e;
            str = str + " " + serverID;
        }
        return str;
    }

    private void startInputThread() {
        inputThread = new Thread(() -> {
            logger.log(Level.INFO, "Starting input thread...");
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    // System.out.println("Change Frequency: ");
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

    private void updateRM() {
        try (AsynchronousSocketChannel server = AsynchronousSocketChannel.open()) {

                String membership = "RM having " + memberCount + " Members: " + getMember();

                String payload = "<" + this.gfdID + ", " + this.RMID + ", membership, " + membership + ", " + memberCount + ">"; // gfd -> rm: <gfdID, rmID , "membership", membership, memberCount>
                byte[] payloadByte = serverInputToByteArr(payload);

                Future<Void> result = server.connect(new InetSocketAddress(this.RMIP, this.RMPort));

                logger.log(Level.INFO, "GFD " + " Connect to " + this.RMID + " IP " + this.RMIP + " Port " + this.RMPort, "RM");

                result.get(1, TimeUnit.SECONDS);
                ByteBuffer buffer = ByteBuffer.wrap(payloadByte);
                Future<Integer> writeval = server.write(buffer);
                writeval.get();

                logger.log(Level.INFO, "Sent " + payload, "RM");


        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }
    
}
