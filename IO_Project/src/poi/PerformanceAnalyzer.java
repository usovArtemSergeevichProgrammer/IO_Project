package poi;

import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Class for testing <strong>single</strong> thread on the functional performance 
 * @author Alex
 * @since 2011
 *
 */
public class PerformanceAnalyzer {

    public final static String APP_LABEL = " APP: ";
    public final static String SeatWebRESTful = "SEAT APP";
    public final static String STARTING_LABEL = "STARTING - ";
    public final static String COMPLETING_LABEL = "COMPLETING - ";
    public final static String NEW_LINE_STR = "\n";
    public final static String ORDER_HAS_BEEN_BROKEN = " PerformanceAnalyzer object was not created. Order of measurement has been broken. Use the first method PerformanceAnalyzer.start(msg)";

    
    private static PerformanceAnalyzer instance = null;
    private static StringBuffer sb = null;
    private static Timestamp startDate = null;
    private static Timestamp endDate = null;
    private static String msg = "";

    private PerformanceAnalyzer(int capacity) {
	startDate = new Timestamp(new Date().getTime());
	// Constructs a new StringBuffer using the specified capacity based on incoming Strings. To avoid INITIAL_SIZE.  
	sb = new StringBuffer(capacity + APP_LABEL.length() + startDate.toString().length() + STARTING_LABEL.length() + NEW_LINE_STR.length());
    }

    public static synchronized void start(String pointMsg) {
	if (instance == null) instance = new PerformanceAnalyzer(msg.length());
	System.out.println(sb.append(APP_LABEL)
		.append(STARTING_LABEL).append(pointMsg)
		.append(startDate).append(". Thread [{")
		.append(Thread.currentThread().getId()).append("} ")
		.append(Thread.currentThread().getName()).append("]")
		.append(NEW_LINE_STR).toString());
	PerformanceAnalyzer.msg = pointMsg; // to store original MSG
	sb.setLength(0);
    }

    public void end(String msg) {
	PerformanceAnalyzer.msg = msg; // to override the original MSG
	end();
    }

    public static synchronized void end() {
	if (instance != null && startDate != null) {
	    endDate = new Timestamp(new Date().getTime());
	    long sec = TimeUnit.MILLISECONDS.toSeconds(endDate.getTime() - startDate.getTime());
	    long min = TimeUnit.MILLISECONDS.toMinutes(endDate.getTime() - startDate.getTime());
	    sb.ensureCapacity(msg.length() + APP_LABEL.length() + endDate.toString().length() + COMPLETING_LABEL.length());
	    System.out.println(sb.append(APP_LABEL).append(COMPLETING_LABEL).append(msg).append(endDate)
		    .append(". It took : ")
		    .append(min).append(" min. ").append(sec % 60).append(" sec.")
		    .append(". Thread [{").append(Thread.currentThread().getId()).append("} ")
		    .append(Thread.currentThread().getName()).append("] ").append(NEW_LINE_STR).toString());
	    sb.setLength(0);
	    instance = null;
	} else {
	    System.err.println( APP_LABEL + ORDER_HAS_BEEN_BROKEN );
	}
    }

    /**
     * @return the startDate
     */
    public static Timestamp getStartDate() {
	return startDate;
    }

    /**
     * @param startDate the startDate to set
     */
    public static void setStartDate(Timestamp startDate) {
	PerformanceAnalyzer.startDate = startDate;
    }

    /**
     * @return the endDate
     */
    public static Timestamp getEndDate() {
	return endDate;
    }

    /**
     * @param endDate the endDate to set
     */
    public static void setEndDate(Timestamp endDate) {
	PerformanceAnalyzer.endDate = endDate;
    }

    /**
     * @return the msg
     */
    public static String getMsg() {
	return msg;
    }

    /**
     * @param msg the msg to set
     */
    public static void setMsg(String msg) {
	PerformanceAnalyzer.msg = msg;
    }

    /**
     * @return the instance
     */
    public static PerformanceAnalyzer getInstance() {
	return instance;
    }

    public static void main(String[] args) {
	
	start("TEST THIS STUFF ");
	try {
	    Thread.sleep(2000); // some job
	} catch (InterruptedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	end();
	
    }
}
