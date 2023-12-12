import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
public class Log {

    private Logger logger;
    private static final SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_ERROR = "\u001B[31m";
    private static final String ANSI_HEARTBEAT = "\u001B[32m";
    private static final String ANSI_WARNING = "\u001B[33m";
    private static final String ANSI_MEMBER = "\u001B[34m";
    private static final String ANSI_CHECKPOINT = "\u001B[35m";
    private static final String ANSI_BACKUP = "\u001B[36m";
    private static final String ANSI_CLIENT = "\u001b[35;1m";
    private static final String ANSI_STATE = "\u001b[34;1m";
    public Log(String className) {
        System.out.println(className);
        logger = Logger.getLogger(className);
    }

    public void log(Level logLevel, String message, boolean heartBeat) {
        message = "[" + currentTime() + "] " + message;
        if (logLevel.equals(Level.INFO)) {
            if (heartBeat) {
                message = ANSI_HEARTBEAT + message + ANSI_RESET;
            } else {
                message = ANSI_RESET + message + ANSI_RESET;
            }
        } else if (logLevel.equals(Level.WARNING)) {
            message = ANSI_WARNING + message + ANSI_RESET;
        } else if (logLevel.equals(Level.SEVERE)) {
            message = ANSI_ERROR + message + ANSI_RESET;
        } 
        logger.log(logLevel, message);
    }

    public void log(Level logLevel, String message) {
        log(logLevel, message, false);
    }

    public void log(Level logLevel, String message, String type) {
        if (type.equals("Member")){
            logger.log(logLevel, ANSI_MEMBER + message + ANSI_RESET);
        } else if (type.equals("Checkpoint")){
            logger.log(logLevel, ANSI_CHECKPOINT + message + ANSI_RESET);
        } else if (type.equals("Backup")) {
            logger.log(logLevel, ANSI_BACKUP + message + ANSI_RESET);
        } else if (type.equals("Client")) {
            logger.log(logLevel, ANSI_CLIENT + message + ANSI_RESET);
        } else if (type.equals("RM")) {
            logger.log(logLevel, ANSI_MEMBER + message + ANSI_RESET);
        } else if (type.equals("State")) {
            logger.log(logLevel, ANSI_STATE + message + ANSI_RESET);
        }
    }

    private String currentTime() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        return sdf3.format(timestamp);
    }

}
