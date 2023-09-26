import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.*;

public class GUDPSocket implements GUDPSocketAPI {
    DatagramSocket datagramSocket;
    HashMap<InetSocketAddress, GUDPEndPoint> gudpendpoints;
    LinkedList<GUDPPacket> querysendgGudpPackets;
    private final LinkedList<DatagramPacket> receiveBuffer = new LinkedList<>();
    Timer timer;
    Thread listenerThread = new ListenerThread();
    static volatile int signal = 0;

    private final HashMap<InetSocketAddress, Integer> receivedAcks = new HashMap<>();

    public GUDPSocket(DatagramSocket socket) {
        datagramSocket = socket;
        gudpendpoints = new HashMap<>();
        querysendgGudpPackets = new LinkedList<>();
        timer = new Timer();
        listenerThread.start();
    }

    private void sendGUDPPacket(GUDPPacket gudppacket) throws IOException {
        try {
            DatagramPacket udppacket = gudppacket.pack();
            datagramSocket.send(udppacket);
        } catch (IOException e) {
            e.printStackTrace();
            // 重新发送数据包
            datagramSocket.send(gudppacket.pack());
        }
    }

    private class ListenerThread extends Thread {
        @Override
        public void run() {
            while (!datagramSocket.isClosed()) {
                try {
                    DatagramPacket packet = new DatagramPacket(new byte[GUDPPacket.MAX_DATAGRAM_LEN],
                            GUDPPacket.MAX_DATAGRAM_LEN);
                    datagramSocket.receive(packet);
                    GUDPPacket gudpPacket = GUDPPacket.unpack(packet);
                    InetSocketAddress remoteAddress = (InetSocketAddress) packet.getSocketAddress();
                    GUDPEndPoint endPoint;
                    synchronized (gudpendpoints) {
                        endPoint = gudpendpoints.get(remoteAddress);
                    }
                    if (gudpPacket.getType() == GUDPPacket.TYPE_ACK) {
                        handleAckPacket(gudpPacket, endPoint);
                    } else if (gudpPacket.getType() == GUDPPacket.TYPE_BSN) {
                        handleBsnPacket(gudpPacket, endPoint);
                    } else if (gudpPacket.getType() == GUDPPacket.TYPE_FIN) {
                        close();
                    } else if (gudpPacket.getType() == GUDPPacket.TYPE_DATA) {
                        synchronized (receiveBuffer) {
                            receiveBuffer.add(packet);
                            receiveBuffer.notify();
                        }

                        GUDPPacket ackPacket = new GUDPPacket(ByteBuffer.allocate(GUDPPacket.HEADER_SIZE));
                        ackPacket.setType(GUDPPacket.TYPE_ACK);
                        ackPacket.setSeqno(gudpPacket.getSeqno() + 1);
                        ackPacket.setSocketAddress(remoteAddress);
                        sendGUDPPacket(ackPacket);
                    } else {
                        System.err.println("Unknown packet type received: " + gudpPacket.getType());
                    }
                } catch (IOException e) {
                    if (!datagramSocket.isClosed()) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public boolean hasReceivedAck(InetSocketAddress address, int seqNumber) {
        synchronized (receivedAcks) {
            return receivedAcks.getOrDefault(address, 0) >= seqNumber;
        }
    }

    private void handleAckPacket(GUDPPacket ackPacket, GUDPEndPoint endPoint) {
        int ackNum = ackPacket.getSeqno();
        InetSocketAddress remoteAddress = endPoint.getRemoteEndPoint();
        synchronized (receivedAcks) {
            receivedAcks.put(remoteAddress, Math.max(receivedAcks.getOrDefault(remoteAddress, 0), ackNum));
        }
        synchronized (querysendgGudpPackets) {
            if (ackNum > endPoint.getBase()) {
                querysendgGudpPackets.removeIf(gudppacket -> gudppacket.getSeqno() < ackNum);
                endPoint.setBase(ackNum);
                endPoint.stopTimer();
                if (endPoint.getBase() < endPoint.getNextseqnum()) {
                    endPoint.startTimer();
                }
            }
        }
        synchronized (sendLock) {
            sendLock.notifyAll();
        }
    }

    private void handleBsnPacket(GUDPPacket bsnPacket, GUDPEndPoint endPoint) throws IOException {
        endPoint.setBase(bsnPacket.getSeqno());
        // 发送ACK包
        GUDPPacket ackPacket = new GUDPPacket(ByteBuffer.allocate(GUDPPacket.HEADER_SIZE));
        ackPacket.setType(GUDPPacket.TYPE_ACK);
        ackPacket.setSeqno(bsnPacket.getSeqno() + 1);
        ackPacket.setSocketAddress(endPoint.getRemoteEndPoint());
        sendGUDPPacket(ackPacket);
    }

    public void sendBsnPacket(InetSocketAddress receiverAddress) throws IOException {
        GUDPEndPoint endPoint = gudpendpoints.compute(receiverAddress, (k, v) -> {
            if (v == null) {
                v = new GUDPEndPoint(receiverAddress.getAddress(), receiverAddress.getPort());
            }
            return v;
        });
        GUDPPacket bsnPacket = new GUDPPacket(ByteBuffer.allocate(GUDPPacket.HEADER_SIZE));
        bsnPacket.setType(GUDPPacket.TYPE_BSN);

        // Randomize the base sequence number
        int randomizedBaseSeqNum = new Random().nextInt(Integer.MAX_VALUE);
        endPoint.setBase(randomizedBaseSeqNum);

        bsnPacket.setSeqno(endPoint.getBase());
        bsnPacket.setSocketAddress(receiverAddress);
        DatagramPacket udpPacket = bsnPacket.pack();
        datagramSocket.send(udpPacket);
    }

    private void startTimer(GUDPEndPoint gudpendpoint) {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                synchronized (querysendgGudpPackets) {
                    for (GUDPPacket gudppacket : querysendgGudpPackets) {
                        if (!hasReceivedAck(gudppacket.getSocketAddress(), gudppacket.getSeqno())) {
                            try {
                                sendGUDPPacket(gudppacket);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                synchronized (sendLock) {
                    sendLock.notifyAll();
                }
            }
        }, 0, gudpendpoint.getTimeoutDuration());
    }

    private final Object sendLock = new Object();

    public void send(DatagramPacket packet) throws IOException {
        /*
         * Send a packet
         * The application put packet in the DatagramPacket format
         * The destination address/port included in the packet
         * Non-blocking – returns immediately
         * GDUP queue packet for future delivery
         */
        InetSocketAddress remSocketAddress = new InetSocketAddress(packet.getAddress(), packet.getPort());
        GUDPEndPoint endPoint = gudpendpoints.computeIfAbsent(remSocketAddress, k -> {
            GUDPEndPoint newEndPoint = new GUDPEndPoint(remSocketAddress.getAddress(), remSocketAddress.getPort());
            try {
                sendBsnPacket(remSocketAddress);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return newEndPoint;
        });
        GUDPPacket gudpPacket = GUDPPacket.encapsulate(packet);
        gudpPacket.setSeqno(endPoint.getNextseqnum());
        synchronized (sendLock) {
            while (endPoint.getNextseqnum() >= endPoint.getBase() + endPoint.getWindowSize()) {
                try {
                    sendLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Sending interrupted", e);
                }
            }
            synchronized (querysendgGudpPackets) {
                querysendgGudpPackets.add(gudpPacket);
                if (endPoint.getNextseqnum() < endPoint.getBase() + endPoint.getWindowSize()) {
                    sendGUDPPacket(gudpPacket);
                    endPoint.setNextseqnum(endPoint.getNextseqnum() + 1);
                    if (endPoint.getBase() == endPoint.getNextseqnum()) {
                        startTimer(endPoint);
                    }
                }
            }
        }

    }

    public void receive(DatagramPacket packet) throws IOException {
        /*
         * Receive a packet
         * The application fetch a packet from GUDP if there is one, otherwise wait
         * until a packet arrives
         * The application handles packets from different senders (which can be
         * differentiated based on the information in the packet
         */
        // byte[] buf = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
        // DatagramPacket udppacket = new DatagramPacket(buf, buf.length);
        // datagramSocket.receive(udppacket);
        // GUDPPacket gudppacket = GUDPPacket.unpack(udppacket);
        // gudppacket.decapsulate(packet);
        synchronized (receiveBuffer) {
            while (receiveBuffer.isEmpty()) {
                try {
                    receiveBuffer.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            DatagramPacket receivedPacket = receiveBuffer.removeFirst();
            System.arraycopy(receivedPacket.getData(), receivedPacket.getOffset(), packet.getData(), packet.getOffset(),
                    receivedPacket.getLength());
            packet.setLength(receivedPacket.getLength());
            packet.setSocketAddress(receivedPacket.getSocketAddress());
        }
    }

    public void finish() throws IOException {
        for (Map.Entry<InetSocketAddress, GUDPEndPoint> entry : gudpendpoints.entrySet()) {
            GUDPEndPoint endPoint = entry.getValue();
            synchronized (querysendgGudpPackets) {
                while (endPoint.getBase() < endPoint.getNextseqnum()) {
                    try {
                        querysendgGudpPackets.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            GUDPPacket finPacket = new GUDPPacket(ByteBuffer.allocate(GUDPPacket.HEADER_SIZE));
            finPacket.setType(GUDPPacket.TYPE_FIN);
            finPacket.setSeqno(endPoint.getNextseqnum());
            finPacket.setSocketAddress(endPoint.getRemoteEndPoint());
            sendGUDPPacket(finPacket);
            gudpendpoints.remove(entry.getKey());
        }
    }

    public void close() throws IOException {
        listenerThread.interrupt();
        try {
            listenerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        datagramSocket.close();
    }

}
