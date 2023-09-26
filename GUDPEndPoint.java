import java.io.IOException;
import java.util.LinkedList;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.TimerTask;
import java.util.Timer;

class GUDPEndPoint {
    /* Pre-defined constant values for key variables */
    public static final int MAX_WINDOW_SIZE = 3;
    public static final int MAX_RETRY = 7;
    public static final long TIMEOUT_DURATION = 3000L; // (3 seconds)

    /* Variables for the control block */
    //private DatagramSocket datagramSocket;
    private InetSocketAddress remoteEndPoint;
    private LinkedList<GUDPPacket> bufferList = new LinkedList<>();

    private int windowSize;
    private int maxRetry;
    private long timeoutDuration;
    private int retry = 0;

    private int base;            // seq of sent packet not yet acked (i.e., base)
    private int nextseqnum;      // seq of next packet to send (i.e., nextseqnum)
    private int last;            // seq of last packet in bufferList
    private int expectedseqnum;  // seq of next packet to receive

    private boolean dropSend = false;        // for drop send packet
    private boolean dropReceive = false;     // for drop receive packet
    private double chance = 0.25;            // drop probability
    
    /* TCP-like states that may be useful to handle the communication */
    protected enum endPointState {
        INIT,
        BSN,
        READY,
        MAXRETRIED,
        FINISHED,
        CLOSED
    }
    private endPointState state = endPointState.INIT;

    /* Custmoized list of events that combined FSM of both GBN sender and receiver */
    public enum readyEvent { 
        INIT,
        WAIT,
        SEND,
        TIMEOUT,
        RECEIVE,
    }
    private readyEvent event = readyEvent.INIT;

    public GUDPEndPoint(InetAddress addr, int port) {
        setRemoteEndPoint(addr, port);
        this.windowSize = MAX_WINDOW_SIZE;
        this.maxRetry = MAX_RETRY;
        this.timeoutDuration = TIMEOUT_DURATION;
    }
 
    public InetSocketAddress getRemoteEndPoint() {
        return this.remoteEndPoint;
    }

    public void setRemoteEndPoint(InetAddress addr, int port) {
        this.remoteEndPoint = new InetSocketAddress(addr, port);
    }

    public int getWindowSize() {
        return this.windowSize;
    }

    public void setWindowSize(int size) {
        this.windowSize = size;
    }

    public int getMaxRetry() {
        return this.maxRetry;
    }

    public void setMaxRetry(int retry) {
        this.maxRetry = retry;
    }

    public long getTimeoutDuration() {
        return this.timeoutDuration;
    }

    public void setTimeoutDuration(long duration) {
        this.timeoutDuration = duration;
    }

    public int getRetry() {
        return this.retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public int getBase() {
        return this.base;
    }

    public void setBase(int ack) {
        this.base = ack;
    }

    public int getNextseqnum() {
        return this.nextseqnum;
    }

    public void setNextseqnum(int seq) {
        this.nextseqnum = seq;
    }

    public int getLast() {
        return this.last;
    }

    public void setLast(int last) {
        this.last = last;
    }

    public int getExpectedseqnum() {
        return this.expectedseqnum;
    }

    public void setExpectedseqnum(int seq) {
        this.expectedseqnum = seq;
    }

    public boolean getDropSend() {
        return this.dropSend;
    }

    public void setDropSend(boolean value) {
        this.dropSend = value;
    }

    public boolean getDropReceive() {
        return this.dropReceive;
    }

    public void setDropReceive(boolean value) {
        this.dropReceive = value;
    }

    public double getChance() {
        return this.chance;
    }

    public void setChance(double value) {
        this.chance = value;
    }

    public endPointState getState() {
        return this.state;
    }

    public void setState(endPointState state) {
        this.state = state;
    }

    public readyEvent getEvent() {
        return this.event;
    }

    public void setEvent(readyEvent event) {
        this.event = event;
    } 

    public void add(GUDPPacket gpacket) {
	    synchronized (bufferList) {
	        bufferList.add(gpacket);
	    }
    }

    public void remove(GUDPPacket gpacket) {
    	synchronized (bufferList) {
    	    bufferList.remove(gpacket);
    	}
    }

    /*
     * Retrieve and remove the first packet from the bufferList
     */
    public GUDPPacket remove() {
    	synchronized (bufferList) {
    	    return bufferList.remove();
    	}
    }

    /*
     * Get the packet with the given sequence number from bufferList
     * IMPORTANT: the packet is still in the bufferList!
     */
    public GUDPPacket getPacket(int seq) {
    	synchronized (bufferList) {
            for (int i = 0; i < bufferList.size(); i++) {
                if (bufferList.get(i).getSeqno() == seq) {
                    return bufferList.get(i);
                }
            }
            return null;
    	}
    }

    /*
     * Remove all packets with sequence number below ACK from bufferList
     * Assuming those packets were successfully received
     */
    public void removeAllACK(int ack) {
    	synchronized (bufferList) {
            while (bufferList.size() > 0) {
                if (bufferList.getFirst().getSeqno() <= ack) {
                    bufferList.removeFirst();
                } else {
                    break;
                }
            }
            //notify();
    	}
    }

    /*
     * Remove all packets from bufferList
     */
    public void removeAll() {
    	synchronized (bufferList) {
            while (bufferList.size() > 0) {
                bufferList.removeFirst();
            }
    	}
    }

    public boolean isEmptyBuffer() {
        if (bufferList.size() == 0)
            return true;
        else
            return false;
    }

    /*
     * Timer uses for sending timeout
     */
    Timer timer;
    public void startTimer() {
        timer = new Timer("Timer");
        TimerTask task = new TimerTask() {
            public void run() {
                System.out.println("TIMEOUT: " + remoteEndPoint.getAddress() + ":" + remoteEndPoint.getPort());
                setEvent(readyEvent.TIMEOUT);
		timer.cancel();
            }
        };
        timer.schedule(task, timeoutDuration);
    }

    public void stopTimer() {
        timer.cancel();
    }

}
