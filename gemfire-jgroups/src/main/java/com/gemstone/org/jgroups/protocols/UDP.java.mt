// $Id: UDP.java,v 1.102 2005/08/29 08:09:05 belaban Exp $

package org.jgroups.protocols;


import EDU.oswego.cs.dl.util.concurrent.BoundedLinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.Executor;
import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.Version;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.BoundedList;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.*;
import java.util.*;


/**
 * IP multicast transport based on UDP. Messages to the group (msg.dest == null) will
 * be multicast (to all group members), whereas point-to-point messages
 * (msg.dest != null) will be unicast to a single member. Uses a multicast and
 * a unicast socket.<p>
 * The following properties are read by the UDP protocol:
 * <ul>
 * <li> param mcast_addr - the multicast address to use; default is 228.8.8.8.
 * <li> param mcast_port - (int) the port that the multicast is sent on; default is 7600
 * <li> param ip_mcast - (boolean) flag whether to use IP multicast; default is true.
 * <li> param ip_ttl - the default time-to-live for multicast packets sent out on this
 * socket; default is 32.
 * <li> param use_packet_handler - boolean, defaults to false.
 * If set, the mcast and ucast receiver threads just put
 * the datagram's payload (a byte buffer) into a queue, from where a separate thread
 * will dequeue and handle them (unmarshal and pass up). This frees the receiver
 * threads from having to do message unmarshalling; this time can now be spent
 * receiving packets. If you have lots of retransmissions because of network
 * input buffer overflow, consider setting this property to true.
 * </ul>
 * @author Bela Ban
 */
public class UDP extends TP implements Runnable {

    /** Socket used for
     * <ol>
     * <li>sending unicast packets and
     * <li>receiving unicast packets
     * </ol>
     * The address of this socket will be our local address (<tt>local_addr</tt>) */
    DatagramSocket  sock=null;

    /**
     * BoundedList<Integer> of the last 100 ports used. This is to avoid reusing a port for DatagramSocket
     */
    private static BoundedList last_ports_used=null;

    /** Maintain a list of local ports opened by DatagramSocket. If this is 0, this option is turned off.
     * If bind_port is null, then this options will be ignored */
    int             num_last_ports=100;

    /** IP multicast socket for <em>receiving</em> multicast packets */
    MulticastSocket mcast_recv_sock=null;

    /** IP multicast socket for <em>sending</em> multicast packets */
    MulticastSocket mcast_send_sock=null;

    /** If we have multiple mcast send sockets, e.g. send_interfaces or send_on_all_interfaces enabled */
    MulticastSocket[] mcast_send_sockets=null;

    /**
     * Traffic class for sending unicast and multicast datagrams.
     * Valid values are (check {@link #DatagramSocket.setTrafficClass(int)}  for details):
     * <UL>
     * <LI><CODE>IPTOS_LOWCOST (0x02)</CODE>, <b>decimal 2</b></LI>
     * <LI><CODE>IPTOS_RELIABILITY (0x04)</CODE><, <b>decimal 4</b>/LI>
     * <LI><CODE>IPTOS_THROUGHPUT (0x08)</CODE>, <b>decimal 8</b></LI>
     * <LI><CODE>IPTOS_LOWDELAY (0x10)</CODE>, <b>decimal</b> 16</LI>
     * </UL>
     */
    int             tos=0; // valid values: 2, 4, 8, 16


    /** The multicast address (mcast address and port) this member uses */
    IpAddress       mcast_addr=null;

    /** The multicast address used for sending and receiving packets */
    String          mcast_addr_name="228.8.8.8";

    /** The multicast port used for sending and receiving packets */
    int             mcast_port=7600;

    /** The multicast receiver thread */
    Thread          mcast_receiver=null;

    /** The unicast receiver thread */
    UcastReceiver   ucast_receiver=null;

    /** Whether to enable IP multicasting. If false, multiple unicast datagram
     * packets are sent rather than one multicast packet */
    boolean         ip_mcast=true;

    /** The time-to-live (TTL) for multicast datagram packets */
    int             ip_ttl=64;

    /** Send buffer size of the multicast datagram socket */
    int             mcast_send_buf_size=32000;

    /** Receive buffer size of the multicast datagram socket */
    int             mcast_recv_buf_size=64000;

    /** Send buffer size of the unicast datagram socket */
    int             ucast_send_buf_size=32000;

    /** Receive buffer size of the unicast datagram socket */
    int             ucast_recv_buf_size=64000;

    long mcast_send_time=0;

    Executor        send_threadpool;


    /** Usually, src addresses are nulled, and the receiver simply sets them to the address of the sender. However,
     * for multiple addresses on a Windows loopback device, this doesn't work
     * (see http://jira.jboss.com/jira/browse/JGRP-79 and the JGroups wiki for details). This must be the same
     * value for all members of the same group. Default is true, for performance reasons */
    // private boolean null_src_addresses=true;



    /**
     * Creates the UDP protocol, and initializes the
     * state variables, does however not start any sockets or threads.
     */
    public UDP() {
    }



    /**
     * Setup the Protocol instance acording to the configuration string.
     * The following properties are read by the UDP protocol:
     * <ul>
     * <li> param mcast_addr - the multicast address to use default is 228.8.8.8
     * <li> param mcast_port - (int) the port that the multicast is sent on default is 7600
     * <li> param ip_mcast - (boolean) flag whether to use IP multicast - default is true
     * <li> param ip_ttl - Set the default time-to-live for multicast packets sent out on this socket. default is 32
     * </ul>
     * @return true if no other properties are left.
     *         false if the properties still have data in them, ie ,
     *         properties are left over and not handled by the protocol stack
     */
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);

        str=props.getProperty("num_last_ports");
        if(str != null) {
            num_last_ports=Integer.parseInt(str);
            props.remove("num_last_ports");
        }

        str=props.getProperty("mcast_addr");
        if(str != null) {
            mcast_addr_name=str;
            props.remove("mcast_addr");
        }

        str=System.getProperty("jboss.partition.udpGroup");
        if(str != null)
            mcast_addr_name=str;

        str=props.getProperty("mcast_port");
        if(str != null) {
            mcast_port=Integer.parseInt(str);
            props.remove("mcast_port");
        }
        str=System.getProperty("jboss.partition.udpPort");
        if(str != null)
            mcast_port=Integer.parseInt(str);

        str=props.getProperty("ip_mcast");
        if(str != null) {
            ip_mcast=Boolean.valueOf(str).booleanValue();
            props.remove("ip_mcast");
        }

        str=props.getProperty("ip_ttl");
        if(str != null) {
            ip_ttl=Integer.parseInt(str);
            props.remove("ip_ttl");
        }

        str=props.getProperty("tos");
        if(str != null) {
            tos=Integer.parseInt(str);
            props.remove("tos");
        }

        str=props.getProperty("mcast_send_buf_size");
        if(str != null) {
            mcast_send_buf_size=Integer.parseInt(str);
            props.remove("mcast_send_buf_size");
        }

        str=props.getProperty("mcast_recv_buf_size");
        if(str != null) {
            mcast_recv_buf_size=Integer.parseInt(str);
            props.remove("mcast_recv_buf_size");
        }

        str=props.getProperty("ucast_send_buf_size");
        if(str != null) {
            ucast_send_buf_size=Integer.parseInt(str);
            props.remove("ucast_send_buf_size");
        }

        str=props.getProperty("ucast_recv_buf_size");
        if(str != null) {
            ucast_recv_buf_size=Integer.parseInt(str);
            props.remove("ucast_recv_buf_size");
        }

        str=props.getProperty("null_src_addresses");
        if(str != null) {
            // null_src_addresses=Boolean.valueOf(str).booleanValue();
            props.remove("null_src_addresses");
            log.error("null_src_addresses has been deprecated, property will be ignored");
        }

        if(props.size() > 0) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }




    private BoundedList getLastPortsUsed() {
        if(last_ports_used == null)
            last_ports_used=new BoundedList(num_last_ports);
        return last_ports_used;
    }



    /* ----------------------- Receiving of MCAST UDP packets ------------------------ */

    public void run() {
        DatagramPacket  packet;
        byte            receive_buf[]=new byte[65535];
        int             offset, len, sender_port;
        byte[]          data;
        InetAddress     sender_addr;
        Address         sender;

        // moved out of loop to avoid excessive object creations (bela March 8 2001)
        packet=new DatagramPacket(receive_buf, receive_buf.length);

        while(mcast_receiver != null && mcast_recv_sock != null) {
            try {
                packet.setData(receive_buf, 0, receive_buf.length);
                mcast_recv_sock.receive(packet);
                sender_addr=packet.getAddress();
                sender_port=packet.getPort();
                offset=packet.getOffset();
                len=packet.getLength();
                data=packet.getData();
                sender=new IpAddress(sender_addr, sender_port);

                if(len > receive_buf.length) {
                    if(log.isErrorEnabled())
                        log.error("size of the received packet (" + len + ") is bigger than " +
                                  "allocated buffer (" + receive_buf.length + "): will not be able to handle packet. " +
                                  "Use the FRAG protocol and make its frag_size lower than " + receive_buf.length);
                }

                receive(mcast_addr, sender, data, offset, len);
            }
            catch(SocketException sock_ex) {
                 if(trace) log.trace("multicast socket is closed, exception=" + sock_ex);
                break;
            }
            catch(InterruptedIOException io_ex) { // thread was interrupted
            }
            catch(Throwable ex) {
                if(log.isErrorEnabled())
                    log.error("failure in multicast receive()", ex);
                Util.sleep(100); // so we don't get into 100% cpu spinning (should NEVER happen !)
            }
        }
        if(log.isDebugEnabled()) log.debug("multicast thread terminated");
    }

    public String getInfo() {
        StringBuffer sb=new StringBuffer();
        sb.append(local_addr).append(" (").append(channel_name).append(')');
        sb.append(" [").append(mcast_addr_name).append(':').append(mcast_port).append("]\n");
        sb.append("Version=").append(Version.description).append(", cvs=\"").append(Version.cvs).append("\"\n");
        sb.append("bound to ").append(bind_addr).append(':').append(bind_port).append('\n');
        sb.append("members: ").append(members).append('\n');
        return sb.toString();
    }

    public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {
        if(ip_mcast && mcast_addr != null) {
            _send(mcast_addr.getIpAddress(), mcast_addr.getPort(), true, data, offset, length);
        }
        else {
            ArrayList mbrs=new ArrayList(members);
            IpAddress mbr;
            for(Iterator it=mbrs.iterator(); it.hasNext();) {
                mbr=(IpAddress)it.next();
                _send(mbr.getIpAddress(), mbr.getPort(), false, data, offset, length);
            }
        }
    }

    public void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception {
        _send(((IpAddress)dest).getIpAddress(), ((IpAddress)dest).getPort(), false, data, offset, length);
    }


    public void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast) {
        msg.setDest(dest);
    }

    public void postUnmarshallingList(Message msg, Address dest, boolean multicast) {
        msg.setDest(dest);
    }

    private void _send(InetAddress dest, int port, boolean mcast, byte[] data, int offset, int length) throws Exception {
        // DatagramPacket packet=new DatagramPacket(data, offset, length, dest, port);

        byte[] copy=new byte[length];
        System.arraycopy(data, offset, copy, 0, length);
        DatagramPacket packet=new DatagramPacket(copy, 0, length, dest, port);

        try {
            if(mcast) {
                if(mcast_send_sock != null) {
                    long tmp_start=System.currentTimeMillis();
                    mcast_send_sock.send(packet);
                    // send_threadpool.execute(new SendTask(mcast_send_sock, packet));
                    mcast_send_time+=(System.currentTimeMillis() - tmp_start);
                }
                else {
                    if(mcast_send_sockets != null) {
                        MulticastSocket s;
                        for(int i=0; i < mcast_send_sockets.length; i++) {
                            s=mcast_send_sockets[i];
                            try {
                                s.send(packet);
                            }
                            catch(Exception e) {
                                log.error("failed sending packet on socket " + s);
                            }
                        }
                    }
                    else {
                        throw new Exception("both mcast_send_sock and mcast_send_sockets are null");
                    }
                }
            }
            else {
                if(sock != null)
                    sock.send(packet);
            }
        }
        catch(Exception ex) {
            Exception new_ex=new Exception("dest=" + dest + ":" + port + " (" + length + " bytes)", ex);
            throw new_ex;
        }
    }

    class SendTask implements Runnable {
        DatagramSocket socket;
        DatagramPacket packet;

        public SendTask(DatagramSocket sock, DatagramPacket packet) {
            this.socket=sock;
            this.packet=packet;
        }

        public void run() {
            try {
                socket.send(packet);
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        }
    }


    /* ------------------------------------------------------------------------------- */



    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return "UDP";
    }





    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    public void start() throws Exception {
        if(log.isDebugEnabled()) log.debug("creating sockets and starting threads");

        send_threadpool=new PooledExecutor(new BoundedLinkedQueue(1024), 8);
        ((PooledExecutor)send_threadpool).setMinimumPoolSize(4);

        createSockets();
        super.start();
        startThreads();
    }


    public void stop() {
        if(log.isDebugEnabled()) log.debug("closing sockets and stopping threads");
        stopThreads();  // will close sockets, closeSockets() is not really needed anymore, but...
        closeSockets(); // ... we'll leave it in there for now (doesn't do anything if already closed)
        super.stop();
        System.out.println("total mcast send time is " + mcast_send_time);
    }





    /*--------------------------- End of Protocol interface -------------------------- */


    /* ------------------------------ Private Methods -------------------------------- */





    /**
     * Create UDP sender and receiver sockets. Currently there are 2 sockets
     * (sending and receiving). This is due to Linux's non-BSD compatibility
     * in the JDK port (see DESIGN).
     */
    private void createSockets() throws Exception {
        InetAddress tmp_addr;

        // bind_addr not set, try to assign one by default. This is needed on Windows

        // changed by bela Feb 12 2003: by default multicast sockets will be bound to all network interfaces

        // CHANGED *BACK* by bela March 13 2003: binding to all interfaces did not result in a correct
        // local_addr. As a matter of fact, comparison between e.g. 0.0.0.0:1234 (on hostA) and
        // 0.0.0.0:1.2.3.4 (on hostB) would fail !
        if(bind_addr == null) {
            InetAddress[] interfaces=InetAddress.getAllByName(InetAddress.getLocalHost().getHostAddress());
            if(interfaces != null && interfaces.length > 0)
                bind_addr=interfaces[0];
        }
        if(bind_addr == null)
            bind_addr=InetAddress.getLocalHost();

        if(bind_addr != null)
            if(log.isInfoEnabled()) log.info("sockets will use interface " + bind_addr.getHostAddress());


        // 2. Create socket for receiving unicast UDP packets. The address and port
        //    of this socket will be our local address (local_addr)
        if(bind_port > 0) {
            sock=createDatagramSocketWithBindPort();
        }
        else {
            sock=createEphemeralDatagramSocket();
        }
        if(tos > 0) {
            try {
                sock.setTrafficClass(tos);
            }
            catch(SocketException e) {
                log.warn("traffic class of " + tos + " could not be set, will be ignored", e);
            }
        }

        if(sock == null)
            throw new Exception("UDP.createSocket(): sock is null");

        local_addr=new IpAddress(sock.getLocalAddress(), sock.getLocalPort());
        if(additional_data != null)
            ((IpAddress)local_addr).setAdditionalData(additional_data);


        // 3. Create socket for receiving IP multicast packets
        if(ip_mcast) {
            // 3a. Create mcast receiver socket
            mcast_recv_sock=new MulticastSocket(mcast_port);
            mcast_recv_sock.setTimeToLive(ip_ttl);
            tmp_addr=InetAddress.getByName(mcast_addr_name);
            mcast_addr=new IpAddress(tmp_addr, mcast_port);

            if(receive_on_all_interfaces || (receive_interfaces != null && receive_interfaces.size() > 0)) {
                List interfaces;
                if(receive_interfaces != null)
                    interfaces=receive_interfaces;
                else
                    interfaces=getAllAvailableInterfaces();
                bindToInterfaces(interfaces, mcast_recv_sock, mcast_addr.getIpAddress());
            }
            else {
                if(bind_addr != null)
                    mcast_recv_sock.setInterface(bind_addr);
                 mcast_recv_sock.joinGroup(tmp_addr);
            }

            // 3b. Create mcast sender socket
            if(send_on_all_interfaces || (send_interfaces != null && send_interfaces.size() > 0)) {
                List interfaces;
                NetworkInterface intf;
                if(send_interfaces != null)
                    interfaces=send_interfaces;
                else
                    interfaces=getAllAvailableInterfaces();
                mcast_send_sockets=new MulticastSocket[interfaces.size()];
                int index=0;
                for(Iterator it=interfaces.iterator(); it.hasNext();) {
                    intf=(NetworkInterface)it.next();
                    mcast_send_sockets[index]=new MulticastSocket();
                    mcast_send_sockets[index].setNetworkInterface(intf);
                    mcast_send_sockets[index].setTimeToLive(ip_ttl);
                    if(tos > 0) {
                        try {
                            mcast_send_sockets[index].setTrafficClass(tos);
                        }
                        catch(SocketException e) {
                            log.warn("traffic class of " + tos + " could not be set, will be ignored", e);
                        }
                    }
                    index++;
                }
            }
            else {
                mcast_send_sock=new MulticastSocket();
                mcast_send_sock.setTimeToLive(ip_ttl);
                if(bind_addr != null)
                    mcast_send_sock.setInterface(bind_addr);

                if(tos > 0) {
                    try {
                        mcast_send_sock.setTrafficClass(tos); // high throughput
                    }
                    catch(SocketException e) {
                        log.warn("traffic class of " + tos + " could not be set, will be ignored", e);
                    }
                }
            }
        }

        setBufferSizes();
        if(log.isInfoEnabled()) log.info("socket information:\n" + dumpSocketInfo());
    }


//    private void bindToAllInterfaces(MulticastSocket s, InetAddress mcastAddr) throws IOException {
//        SocketAddress tmp_mcast_addr=new InetSocketAddress(mcastAddr, mcast_port);
//        Enumeration en=NetworkInterface.getNetworkInterfaces();
//        while(en.hasMoreElements()) {
//            NetworkInterface i=(NetworkInterface)en.nextElement();
//            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
//                InetAddress addr=(InetAddress)en2.nextElement();
//                // if(addr.isLoopbackAddress())
//                // continue;
//                s.joinGroup(tmp_mcast_addr, i);
//                if(trace)
//                    log.trace("joined " + tmp_mcast_addr + " on interface " + i.getName() + " (" + addr + ")");
//                break;
//            }
//        }
//    }


    /**
     *
     * @param interfaces List<NetworkInterface>. Guaranteed to have no duplicates
     * @param s
     * @param mcastAddr
     * @throws IOException
     */
    private void bindToInterfaces(List interfaces, MulticastSocket s, InetAddress mcastAddr) throws IOException {
        SocketAddress tmp_mcast_addr=new InetSocketAddress(mcastAddr, mcast_port);
        for(Iterator it=interfaces.iterator(); it.hasNext();) {
            NetworkInterface i=(NetworkInterface)it.next();
            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr=(InetAddress)en2.nextElement();
                s.joinGroup(tmp_mcast_addr, i);
                if(trace)
                    log.trace("joined " + tmp_mcast_addr + " on interface " + i.getName() + " (" + addr + ")");
                break;
            }
        }
    }

    private List getAllAvailableInterfaces() throws SocketException {
        List retval=new ArrayList(10);
        NetworkInterface intf;
        for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
            intf=(NetworkInterface)en.nextElement();
            retval.add(intf);
        }
        return retval;
    }


    /** Creates a DatagramSocket with a random port. Because in certain operating systems, ports are reused,
     * we keep a list of the n last used ports, and avoid port reuse */
    private DatagramSocket createEphemeralDatagramSocket() throws SocketException {
        DatagramSocket tmp;
        int localPort=0;
        while(true) {
            tmp=new DatagramSocket(localPort, bind_addr); // first time localPort is 0
            if(num_last_ports <= 0)
                break;
            localPort=tmp.getLocalPort();
            if(getLastPortsUsed().contains(new Integer(localPort))) {
                if(log.isDebugEnabled())
                    log.debug("local port " + localPort + " already seen in this session; will try to get other port");
                try {tmp.close();} catch(Throwable e) {}
                localPort++;
            }
            else {
                getLastPortsUsed().add(new Integer(localPort));
                break;
            }
        }
        return tmp;
    }




    /**
     * Creates a DatagramSocket when bind_port > 0. Attempts to allocate the socket with port == bind_port, and
     * increments until it finds a valid port, or until port_range has been exceeded
     * @return DatagramSocket The newly created socket
     * @throws Exception
     */
    private DatagramSocket createDatagramSocketWithBindPort() throws Exception {
        DatagramSocket tmp=null;
        // 27-6-2003 bgooren, find available port in range (start_port, start_port+port_range)
        int rcv_port=bind_port, max_port=bind_port + port_range;
        while(rcv_port <= max_port) {
            try {
                tmp=new DatagramSocket(rcv_port, bind_addr);
                break;
            }
            catch(SocketException bind_ex) {	// Cannot listen on this port
                rcv_port++;
            }
            catch(SecurityException sec_ex) { // Not allowed to listen on this port
                rcv_port++;
            }

            // Cannot listen at all, throw an Exception
            if(rcv_port >= max_port + 1) { // +1 due to the increment above
                throw new Exception("UDP.createSockets(): cannot listen on any port in range " +
                        bind_port + '-' + (bind_port + port_range));
            }
        }
        return tmp;
    }


    private String dumpSocketInfo() throws Exception {
        StringBuffer sb=new StringBuffer(128);
        sb.append("local_addr=").append(local_addr);
        sb.append(", mcast_addr=").append(mcast_addr);
        sb.append(", bind_addr=").append(bind_addr);
        sb.append(", ttl=").append(ip_ttl);

        if(sock != null) {
            sb.append("\nsock: bound to ");
            sb.append(sock.getLocalAddress().getHostAddress()).append(':').append(sock.getLocalPort());
            sb.append(", receive buffer size=").append(sock.getReceiveBufferSize());
            sb.append(", send buffer size=").append(sock.getSendBufferSize());
        }

        if(mcast_recv_sock != null) {
            sb.append("\nmcast_recv_sock: bound to ");
            sb.append(mcast_recv_sock.getInterface().getHostAddress()).append(':').append(mcast_recv_sock.getLocalPort());
            sb.append(", send buffer size=").append(mcast_recv_sock.getSendBufferSize());
            sb.append(", receive buffer size=").append(mcast_recv_sock.getReceiveBufferSize());
        }

         if(mcast_send_sock != null) {
            sb.append("\nmcast_send_sock: bound to ");
            sb.append(mcast_send_sock.getInterface().getHostAddress()).append(':').append(mcast_send_sock.getLocalPort());
            sb.append(", send buffer size=").append(mcast_send_sock.getSendBufferSize());
            sb.append(", receive buffer size=").append(mcast_send_sock.getReceiveBufferSize());
        }
        if(mcast_send_sockets != null) {
            sb.append("\n").append(mcast_send_sockets.length).append(" mcast send sockets:\n");
            MulticastSocket s;
            for(int i=0; i < mcast_send_sockets.length; i++) {
                s=mcast_send_sockets[i];
                sb.append(s.getInterface().getHostAddress()).append(':').append(s.getLocalPort());
                sb.append(", send buffer size=").append(s.getSendBufferSize());
                sb.append(", receive buffer size=").append(s.getReceiveBufferSize()).append("\n");
            }
        }
        return sb.toString();
    }


    void setBufferSizes() {
        if(sock != null)
            setBufferSize(sock, ucast_send_buf_size, ucast_recv_buf_size);

        if(mcast_recv_sock != null)
            setBufferSize(mcast_recv_sock, mcast_send_buf_size, mcast_recv_buf_size);

        if(mcast_send_sock != null)
            setBufferSize(mcast_send_sock, mcast_send_buf_size, mcast_recv_buf_size);

        if(mcast_send_sockets != null) {
            for(int i=0; i < mcast_send_sockets.length; i++) {
                setBufferSize(mcast_send_sockets[i], mcast_send_buf_size, mcast_recv_buf_size);
            }
        }
    }

    private void setBufferSize(DatagramSocket sock, int send_buf_size, int recv_buf_size) {
        try {
            sock.setSendBufferSize(send_buf_size);
        }
        catch(Throwable ex) {
            if(warn) log.warn("failed setting send buffer size of " + send_buf_size + " in " + sock + ": " + ex);
        }

        try {
            sock.setReceiveBufferSize(recv_buf_size);
        }
        catch(Throwable ex) {
            if(warn) log.warn("failed setting receive buffer size of " + recv_buf_size + " in " + sock + ": " + ex);
        }
    }


    /**
     * Closed UDP unicast and multicast sockets
     */
    void closeSockets() {
        // 1. Close multicast socket
        closeMulticastSocket();

        // 2. Close socket
        closeSocket();
    }


    void closeMulticastSocket() {
        if(mcast_recv_sock != null) {
            try {
                if(mcast_addr != null) {
                    mcast_recv_sock.leaveGroup(mcast_addr.getIpAddress());
                }
                mcast_recv_sock.close(); // this will cause the mcast receiver thread to break out of its loop
                mcast_recv_sock=null;
                if(log.isDebugEnabled()) log.debug("multicast receive socket closed");
            }
            catch(IOException ex) {
            }
            mcast_addr=null;
        }

        if(mcast_send_sock != null) {
            mcast_send_sock.close();
            mcast_send_sock=null;
            if(log.isDebugEnabled()) log.debug("multicast send socket closed");
        }
        if(mcast_send_sockets != null) {
            MulticastSocket s;
            for(int i=0; i < mcast_send_sockets.length; i++) {
                s=mcast_send_sockets[i];
                s.close();
                if(log.isDebugEnabled()) log.debug("multicast send socket " + s + " closed");
            }
            mcast_send_sockets=null;
        }
    }


    private void closeSocket() {
        if(sock != null) {
            sock.close();
            sock=null;
            if(log.isDebugEnabled()) log.debug("socket closed");
        }
    }




    /**
     * Starts the unicast and multicast receiver threads
     */
    void startThreads() throws Exception {
        if(ucast_receiver == null) {
            //start the listener thread of the ucast_recv_sock
            ucast_receiver=new UcastReceiver();
            ucast_receiver.start();
             if(log.isDebugEnabled()) log.debug("created unicast receiver thread");
        }

        if(ip_mcast) {
            if(mcast_receiver != null) {
                if(mcast_receiver.isAlive()) {
                    if(log.isDebugEnabled()) log.debug("did not create new multicastreceiver thread as existing " +
                                                       "multicast receiver thread is still running");
                }
                else
                    mcast_receiver=null; // will be created just below...
            }

            if(mcast_receiver == null) {
                mcast_receiver=new Thread(this, "UDP mcast receiver");
                mcast_receiver.setPriority(Thread.MAX_PRIORITY); // needed ????
                mcast_receiver.setDaemon(true);
                mcast_receiver.start();
            }
        }
    }


    /**
     * Stops unicast and multicast receiver threads
     */
    void stopThreads() {
        Thread tmp;

        // 1. Stop the multicast receiver thread
        if(mcast_receiver != null) {
            if(mcast_receiver.isAlive()) {
                tmp=mcast_receiver;
                mcast_receiver=null;
                closeMulticastSocket();  // will cause the multicast thread to terminate
                tmp.interrupt();
                try {
                    tmp.join(100);
                }
                catch(Exception e) {
                }
                tmp=null;
            }
            mcast_receiver=null;
        }

        // 2. Stop the unicast receiver thread
        if(ucast_receiver != null) {
            ucast_receiver.stop();
            ucast_receiver=null;
        }
    }



    protected void handleConfigEvent(HashMap map) {
        super.handleConfigEvent(map);
        if(map == null) return;
        if(map.containsKey("send_buf_size")) {
            mcast_send_buf_size=((Integer)map.get("send_buf_size")).intValue();
            ucast_send_buf_size=mcast_send_buf_size;
        }
        if(map.containsKey("recv_buf_size")) {
            mcast_recv_buf_size=((Integer)map.get("recv_buf_size")).intValue();
            ucast_recv_buf_size=mcast_recv_buf_size;
        }
        setBufferSizes();
    }


//    private void nullAddresses(Message msg, IpAddress dest, IpAddress src) {
//        if(src != null) {
//            if(null_src_addresses)
//                msg.setSrc(new IpAddress(src.getPort(), false));  // null the host part, leave the port
//            if(src.getAdditionalData() != null)
//                ((IpAddress)msg.getSrc()).setAdditionalData(src.getAdditionalData());
//        }
//        else if(dest != null && !dest.isMulticastAddress()) { // unicast
//            msg.setSrc(null);
//        }
//    }

  //  private void setAddresses(Message msg, Address dest) {
   //     msg.setDest(dest);
//
//        // set the source address if not set
//        IpAddress src_addr=(IpAddress)msg.getSrc();
//        if(src_addr == null) {
//            msg.setSrc(sender);
//        }
//        else {
//            byte[] tmp_additional_data=src_addr.getAdditionalData();
//            if(src_addr.getIpAddress() == null) {
//                IpAddress tmp=new IpAddress(sender.getIpAddress(), src_addr.getPort());
//                msg.setSrc(tmp);
//            }
//            if(tmp_additional_data != null)
//                ((IpAddress)msg.getSrc()).setAdditionalData(tmp_additional_data);
//        }
 //   }



    /* ----------------------------- End of Private Methods ---------------------------------------- */

    /* ----------------------------- Inner Classes ---------------------------------------- */




    public class UcastReceiver implements Runnable {
        boolean running=true;
        Thread thread=null;


        public void start() {
            if(thread == null) {
                thread=new Thread(this, "UDP.UcastReceiverThread");
                thread.setDaemon(true);
                running=true;
                thread.start();
            }
        }


        public void stop() {
            Thread tmp;
            if(thread != null && thread.isAlive()) {
                running=false;
                tmp=thread;
                thread=null;
                closeSocket(); // this will cause the thread to break out of its loop
                tmp.interrupt();
                tmp=null;
            }
            thread=null;
        }


        public void run() {
            DatagramPacket  packet;
            byte            receive_buf[]=new byte[65535];
            int             offset, len;
            byte[]          data;
            InetAddress     sender_addr;
            int             sender_port;
            Address         sender;

            // moved out of loop to avoid excessive object creations (bela March 8 2001)
            packet=new DatagramPacket(receive_buf, receive_buf.length);

            while(running && thread != null && sock != null) {
                try {
                    packet.setData(receive_buf, 0, receive_buf.length);
                    sock.receive(packet);
                    sender_addr=packet.getAddress();
                    sender_port=packet.getPort();
                    offset=packet.getOffset();
                    len=packet.getLength();
                    data=packet.getData();
                    sender=new IpAddress(sender_addr, sender_port);

                    if(len > receive_buf.length) {
                        if(log.isErrorEnabled())
                            log.error("size of the received packet (" + len + ") is bigger than allocated buffer (" +
                                      receive_buf.length + "): will not be able to handle packet. " +
                                      "Use the FRAG protocol and make its frag_size lower than " + receive_buf.length);
                    }
                    receive(local_addr, sender, data, offset, len);
                }
                catch(SocketException sock_ex) {
                    if(log.isDebugEnabled()) log.debug("unicast receiver socket is closed, exception=" + sock_ex);
                    break;
                }
                catch(InterruptedIOException io_ex) { // thread was interrupted
                }
                catch(Throwable ex) {
                    if(log.isErrorEnabled())
                        log.error("[" + local_addr + "] failed receiving unicast packet", ex);
                    Util.sleep(100); // so we don't get into 100% cpu spinning (should NEVER happen !)
                }
            }
            if(log.isDebugEnabled()) log.debug("unicast receiver thread terminated");
        }
    }

}
