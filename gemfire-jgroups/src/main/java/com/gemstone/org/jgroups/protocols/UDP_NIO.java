/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.protocols;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.LogicalAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Queue;
import com.gemstone.org.jgroups.util.QueueClosedException;
import com.gemstone.org.jgroups.util.Util;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Multicast transport. Similar to UDP, but binds to multiple (or all) interfaces for sending and receiving
 * multicast and unicast traffic.<br/>
 * The list of interfaces can be set via a property (comma-delimited list of IP addresses or "all" for all
 * interfaces). Note that this class only works under JDK 1.4 and higher.<br/>
 * For each of the interfaces listed we create a Listener, which listens on the group multicast address and creates
 * a unicast datagram socket. The address of this member is determined at startup time, and is the host name plus
 * a timestamp (LogicalAddress). It does not change during the lifetime of the process. The LogicalAddress contains
 * a list of all unicast socket addresses to which we can send back unicast messages. When we send a message, the
 * Listener adds the sender's return address. When we receive a message, we add that address to our routing cache, which
 * contains logical addresses and physical addresses. When we need to send a unicast address, we first check whether
 * the logical address has a physical address associated with it in the cache. If so, we send a message to that address.
 * If not, we send the unicast message to <em>all</em> physical addresses contained in the LogicalAddress.<br/>
 * UDP_NIO guarantees that - in scenarios with multiple subnets and multi-homed machines - members do see each other.
 * There is some overhead in multicasting the same message on multiple interfaces, and potentially sending a unicast
 * on multiple interfaces as well, but the advantage is that we don't need stuff like bind_addr any longer. Plus,
 * the unicast routing caches should ensure that unicasts are only sent via 1 interface in almost all cases.
 * 
 * @author Bela Ban Oct 2003
 * @version $Id: UDP_NIO.java,v 1.4 2005/08/11 12:43:47 belaban Exp $
 */
public class UDP_NIO extends Protocol implements Receiver {

    static final String name="UDP_NIO";

    /** Maintains a list of Connectors, one for each interface we're listening on */
    ConnectorTable ct=null;

    /** A List<String> of bind addresses, we create 1 Connector for each interface */
    List bind_addrs=null;

    /** The name of the group to which this member is connected */
    String group_name=null;

    /** The multicast address (mcast address and port) this member uses (default: 230.1.2.3:7500) */
    InetSocketAddress mcast_addr=null;

    /** The address of this member. Valid for the lifetime of the JVM in which this member runs */
    LogicalAddress local_addr=new LogicalAddress(null, null);

    /** Logical address without list of physical addresses */
    LogicalAddress local_addr_canonical=local_addr.copy();

    /** Pre-allocated byte stream. Used for serializing datagram packets */
    ByteArrayOutputStream out_stream=new ByteArrayOutputStream(65535);

    /**
     * The port to which the unicast receiver socket binds.
     * 0 means to bind to any (ephemeral) port
     */
    int local_bind_port=0;
    int port_range=1; // 27-6-2003 bgooren, Only try one port by default


    /**
     * Whether to enable IP multicasting. If false, multiple unicast datagram
     * packets are sent rather than one multicast packet
     */
    boolean ip_mcast=true;

    /** The time-to-live (TTL) for multicast datagram packets */
    int ip_ttl=32;

    /** The members of this group (updated when a member joins or leaves) */
    Vector members=new Vector();

    /**
     * Header to be added to all messages sent via this protocol. It is
     * preallocated for efficiency
     */
    UdpHeader udp_hdr=null;

    /** Send buffer size of the multicast datagram socket */
    int mcast_send_buf_size=300000;

    /** Receive buffer size of the multicast datagram socket */
    int mcast_recv_buf_size=300000;

    /** Send buffer size of the unicast datagram socket */
    int ucast_send_buf_size=300000;

    /** Receive buffer size of the unicast datagram socket */
    int ucast_recv_buf_size=300000;

    /**
     * If true, messages sent to self are treated specially: unicast messages are
     * looped back immediately, multicast messages get a local copy first and -
     * when the real copy arrives - it will be discarded. Useful for Window
     * media (non)sense
     * @deprecated This is used by default now
     */
//    boolean loopback=true; //todo: remove GemStoneAddition(omitted)

    /**
     * Sometimes receivers are overloaded (they have to handle de-serialization etc).
     * Packet handler is a separate thread taking care of de-serialization, receiver
     * thread(s) simply put packet in queue and return immediately. Setting this to
     * true adds one more thread
     */
    boolean use_packet_handler=false;

    /** Used by packet handler to store incoming DatagramPackets */
    Queue packet_queue=null;

    /**
     * If set it will be added to <tt>local_addr</tt>. Used to implement
     * for example transport independent addresses
     */
    byte[] additional_data=null;

    /**
     * Dequeues DatagramPackets from packet_queue, unmarshalls them and
     * calls <tt>handleIncomingUdpPacket()</tt>
     */
    PacketHandler packet_handler=null;


    /** Number of bytes to allocate to receive a packet. Needs to be set to be higher than frag_size
     * (handle CONFIG event)
     */
    static final int DEFAULT_RECEIVE_BUFFER_SIZE=120000;  // todo: make settable and/or use CONFIG event




    /**
     * Creates the UDP_NIO protocol, and initializes the
     * state variables, does however not start any sockets or threads.
     */
    public UDP_NIO() {
    }

    /**
     * debug only
     */
    @Override // GemStoneAddition  
    public String toString() {
        return "Protocol UDP(local address: " + local_addr + ')';
    }


    public void receive(DatagramPacket packet) {
        int           len=packet.getLength();
        byte[]        data=packet.getData();
        SocketAddress sender=packet.getSocketAddress();

        if(len == 4) {  // received a diagnostics probe
            if(data[0] == 'd' && data[1] == 'i' && data[2] == 'a' && data[3] == 'g') {
                handleDiagnosticProbe(sender);
                return;
            }
        }

        if(trace)
            log.trace("received " + len + " bytes from " + sender);

        if(use_packet_handler && packet_queue != null) {
            byte[] tmp=new byte[len];
            System.arraycopy(data, 0, tmp, 0, len);
            try {
                Object[] arr=new Object[]{tmp, sender};
                packet_queue.add(arr);
                return;
            }
            catch(QueueClosedException e) {
                if(warn) log.warn("packet queue for packet handler thread is closed");
                // pass through to handleIncomingPacket()
            }
        }

        handleIncomingUdpPacket(data, sender);
    }


    /* ----------------------- Receiving of MCAST UDP packets ------------------------ */

//    public void run() {
//        DatagramPacket packet;
//        byte receive_buf[]=new byte[65000];
//        int len;
//        byte[] tmp1, tmp2;
//
//        // moved out of loop to avoid excessive object creations (bela March 8 2001)
//        packet=new DatagramPacket(receive_buf, receive_buf.length);
//
//        while(mcast_receiver != null && mcast_sock != null) {
//            try {
//                packet.setData(receive_buf, 0, receive_buf.length);
//                mcast_sock.receive(packet);
//                len=packet.getLength();
//                if(len == 1 && packet.getData()[0] == 0) {
//                    if(trace) if(log.isInfoEnabled()) log.info("UDP_NIO.run()", "received dummy packet");
//                    continue;
//                }
//
//                if(len == 4) {  // received a diagnostics probe
//                    byte[] tmp=packet.getData();
//                    if(tmp[0] == 'd' && tmp[1] == 'i' && tmp[2] == 'a' && tmp[3] == 'g') {
//                        handleDiagnosticProbe(null, null);
//                        continue;
//                    }
//                }
//
//                if(trace)
//                    if(log.isInfoEnabled()) log.info("UDP_NIO.receive()", "received (mcast) " + packet.getLength() + " bytes from " +
//                            packet.getAddress() + ":" + packet.getPort() + " (size=" + len + " bytes)");
//                if(len > receive_buf.length) {
//                    if(log.isErrorEnabled()) log.error("UDP_NIO.run()", "size of the received packet (" + len + ") is bigger than " +
//                            "allocated buffer (" + receive_buf.length + "): will not be able to handle packet. " +
//                            "Use the FRAG protocol and make its frag_size lower than " + receive_buf.length);
//                }
//
//                if(Version.compareTo(packet.getData()) == false) {
//                    if(warn) log.warn("UDP_NIO.run()",
//                            "packet from " + packet.getAddress() + ":" + packet.getPort() +
//                            " has different version (" +
//                            Version.printVersionId(packet.getData(), Version.version_id.length) +
//                            ") from ours (" + Version.printVersionId(Version.version_id) +
//                            "). This may cause problems");
//                }
//
//                if(use_packet_handler) {
//                    tmp1=packet.getData();
//                    tmp2=new byte[len];
//                    System.arraycopy(tmp1, 0, tmp2, 0, len);
//                    packet_queue.add(tmp2);
//                } else
//                    handleIncomingUdpPacket(packet.getData());
//            } catch(SocketException sock_ex) {
//                 if(log.isInfoEnabled()) log.info("UDP_NIO.run()", "multicast socket is closed, exception=" + sock_ex);
//                break;
//            } catch(InterruptedIOException io_ex) { // thread was interrupted
//                ; // go back to top of loop, where we will terminate loop
//            } catch(Throwable ex) {
//                if(log.isErrorEnabled()) log.error("UDP_NIO.run()", "exception=" + ex + ", stack trace=" + Util.printStackTrace(ex));
//                Util.sleep(1000); // so we don't get into 100% cpu spinning (should NEVER happen !)
//            }
//        }
//         if(log.isInfoEnabled()) log.info("UDP_NIO.run()", "multicast thread terminated");
//    }

    void handleDiagnosticProbe(SocketAddress sender) {
        try {
            byte[] diag_rsp=getDiagResponse().getBytes();
            DatagramPacket rsp=new DatagramPacket(diag_rsp, 0, diag_rsp.length, sender);

                if(log.isInfoEnabled()) log.info(ExternalStrings.UDP_NIO_SENDING_DIAG_RESPONSE_TO__0, sender);
            ct.send(rsp);
        } 
        catch(Exception t) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.UDP_NIO_FAILED_SENDING_DIAG_RSP_TO__0__EXCEPTION_1, new Object[] {sender, t});
        }
    }

    String getDiagResponse() {
        StringBuffer sb=new StringBuffer();
        sb.append(local_addr).append(" (").append(group_name).append(')');
        sb.append(" [").append(mcast_addr).append("]\n");
        sb.append("Version=").append(JGroupsVersion.description).append(", cvs=\"").append(JGroupsVersion.cvs).append("\"\n");
        sb.append("physical addresses: ").append(local_addr.getPhysicalAddresses()).append('\n');
        sb.append("members: ").append(members).append('\n');

        return sb.toString();
    }

    /* ------------------------------------------------------------------------------- */



    /*------------------------------ Protocol interface ------------------------------ */

    @Override // GemStoneAddition  
    public String getName() {
        return name;
    }


    @Override // GemStoneAddition  
    public void init() throws Exception {
        if(use_packet_handler) {
            packet_queue=new Queue();
            packet_handler=new PacketHandler();
        }
    }


    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    @Override // GemStoneAddition  
    public void start() throws Exception {
         if(log.isInfoEnabled()) log.info(ExternalStrings.UDP_NIO_CREATING_SOCKETS_AND_STARTING_THREADS);
        if(ct == null) {
            ct=new ConnectorTable(mcast_addr, DEFAULT_RECEIVE_BUFFER_SIZE, mcast_recv_buf_size, ip_mcast, this);

            for(Iterator it=bind_addrs.iterator(); it.hasNext();) {
                String bind_addr=(String)it.next();
                ct.listenOn(bind_addr, local_bind_port, port_range, DEFAULT_RECEIVE_BUFFER_SIZE, ucast_recv_buf_size,
                        ucast_send_buf_size, ip_ttl, this);
            }

            // add physical addresses to local_addr
            List physical_addrs=ct.getConnectorAddresses(); // must be non-null and size() >= 1
            for(Iterator it=physical_addrs.iterator(); it.hasNext();) {
                SocketAddress address=(SocketAddress)it.next();
                local_addr.addPhysicalAddress(address);
            }

            if(additional_data != null)
                local_addr.setAdditionalData(additional_data);

            ct.start();

            passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
            if(use_packet_handler)
            packet_handler.start();
        }
    }


    @Override // GemStoneAddition  
    public void stop() {
         if(log.isInfoEnabled()) log.info(ExternalStrings.UDP_NIO_CLOSING_SOCKETS_AND_STOPPING_THREADS);
        if(packet_handler != null)
            packet_handler.stop();
        if(ct != null) {
            ct.stop();
            ct=null;
        }
        local_addr.removeAllPhysicalAddresses();
    }


    /**
     * Setup the Protocol instance acording to the configuration string.
     * The following properties are being read by the UDP protocol:
     * <ul>
     * <li> param mcast_addr - the multicast address to use default is 224.0.0.200
     * <li> param mcast_port - (int) the port that the multicast is sent on default is 7500
     * <li> param ip_mcast - (boolean) flag whether to use IP multicast - default is true
     * <li> param ip_ttl - Set the default time-to-live for multicast packets sent out on this socket. default is 32
     * </ul>
     * @return true if no other properties are left.
     *         false if the properties still have data in them, ie ,
     *         properties are left over and not handled by the protocol stack
     */
    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String  str;
        List    exclude_list=null;
        String  mcast_addr_name="230.8.8.8";
        int     mcast_port=7500;

        super.setProperties(props);
        str=props.getProperty("bind_addrs");
        if(str != null) {
            str=str.trim();
            if("all".equals(str.toLowerCase())) {
                try {
                    bind_addrs=determineAllBindInterfaces();
                }
                catch(SocketException e) {
                    e.printStackTrace();
                    bind_addrs=null;
                }
            }
            else {
                bind_addrs=Util.parseCommaDelimitedStrings(str);
            }
            props.remove("bind_addrs");
        }

        str=props.getProperty("bind_addrs_exclude");
        if(str != null) {
            str=str.trim();
            exclude_list=Util.parseCommaDelimitedStrings(str);
            props.remove("bind_addrs_exclude");
        }

        str=props.getProperty("bind_port");
        if(str != null) {
            local_bind_port=Integer.parseInt(str);
            props.remove("bind_port");
        }

        str=props.getProperty("start_port");
        if(str != null) {
            local_bind_port=Integer.parseInt(str);
            props.remove("start_port");
        }

        str=props.getProperty("port_range");
        if(str != null) {
            port_range=Integer.parseInt(str);
            props.remove("port_range");
        }

        str=props.getProperty("mcast_addr");
        if(str != null) {
            mcast_addr_name=str;
            props.remove("mcast_addr");
        }

        str=props.getProperty("mcast_port");
        if(str != null) {
            mcast_port=Integer.parseInt(str);
            props.remove("mcast_port");
        }

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

        str=props.getProperty("use_packet_handler");
        if(str != null) {
            use_packet_handler=Boolean.valueOf(str).booleanValue();
            props.remove("use_packet_handler");
        }


        // determine mcast_addr
        mcast_addr=new InetSocketAddress(mcast_addr_name, mcast_port);

        // handling of bind_addrs
        if(bind_addrs == null)
            bind_addrs=new ArrayList();
        if(bind_addrs.size() == 0) {
            try {
                String default_bind_addr=determineDefaultBindInterface();
                bind_addrs.add(default_bind_addr);
            }
            catch(SocketException ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.UDP_NIO_FAILED_DETERMINING_THE_DEFAULT_BIND_INTERFACE__0, ex);
            }
        }
        if(exclude_list != null) {
            bind_addrs.removeAll(exclude_list);
        }
        if(bind_addrs.size() == 0) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.UDP_NIO_NO_VALID_BIND_INTERFACE_FOUND_UNABLE_TO_LISTEN_FOR_NETWORK_TRAFFIC);
            return false;
        }
        else {

                if(log.isInfoEnabled()) log.info(ExternalStrings.UDP_NIO_BIND_INTERFACES_ARE__0, bind_addrs);
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.UDP_NIO_UDP_NIOSETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }


    /**
     * This prevents the up-handler thread to be created, which essentially is superfluous:
     * messages are received from the network rather than from a layer below.
     * DON'T REMOVE ! 
     */
    @Override // GemStoneAddition  
    public void startUpHandler() {
    }

    /**
     * handle the UP event.
     * 
     * @param evt - the event being send from the stack
     */
    @Override // GemStoneAddition  
    public void up(Event evt) {
        passUp(evt);

        switch(evt.getType()) {

            case Event.CONFIG:
                passUp(evt);
                 if(log.isInfoEnabled()) log.info(ExternalStrings.UDP_NIO_RECEIVED_CONFIG_EVENT__0, evt.getArg());
                handleConfigEvent((HashMap)evt.getArg());
                return;
        }

        passUp(evt);
    }

    /**
     * Caller by the layer above this layer. Usually we just put this Message
     * into the send queue and let one or more worker threads handle it. A worker thread
     * then removes the Message from the send queue, performs a conversion and adds the
     * modified Message to the send queue of the layer below it, by calling Down).
     */
    @Override // GemStoneAddition  
    public void down(Event evt) {
        Message msg;
        Object dest_addr;

        if(evt.getType() != Event.MSG) {  // unless it is a message handle it and respond
            handleDownEvent(evt);
            return;
        }

        msg=(Message)evt.getArg();

        if(udp_hdr != null && udp_hdr.channel_name != null) {
            // added patch by Roland Kurmann (March 20 2003)
            msg.putHeader(name, udp_hdr);
        }

        dest_addr=msg.getDest();

        // Because we don't call Protocol.passDown(), we notify the observer directly (e.g. PerfObserver).
        // This way, we still have performance numbers for UDP
        if(observer != null)
            observer.passDown(evt);

        if(dest_addr == null) { // 'null' means send to all group members
            if(ip_mcast == false) {
                //sends a separate UDP message to each address
                sendMultipleUdpMessages(msg, members);
                return;
            }
        }

        try {
            sendUdpMessage(msg); // either unicast (dest != null) or multicast (dest == null)
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.UDP_NIO_EXCEPTION_0__MSG_1__MCAST_ADDR_2, new Object[] {e, msg, mcast_addr});
        }
    }






    /*--------------------------- End of Protocol interface -------------------------- */


    /* ------------------------------ Private Methods -------------------------------- */


    void handleMessage(Message msg) {

    }


    /**
     * Processes a packet read from either the multicast or unicast socket. Needs to be synchronized because
     * mcast or unicast socket reads can be concurrent
     */
    void handleIncomingUdpPacket(byte[] data, SocketAddress sender) {
        ByteArrayInputStream inp_stream;
        ObjectInputStream    inp;
        Message              msg=null;
        UdpHeader            hdr=null;
        Event                evt;
        Address              dst, src;
        short                version;

        try {
            // skip the first n bytes (default: 4), this is the version info
            inp_stream=new ByteArrayInputStream(data);
            inp=new ObjectInputStream(inp_stream);
            version=inp.readShort();

            if(JGroupsVersion.compareTo(version) == false) {
                if(warn)
                    log.warn("packet from " + sender + " has different version (" + version +
                               ") from ours (" + JGroupsVersion.version + "). This may cause problems");
            }

            msg=new Message();
            msg.readExternal(inp);
            dst=msg.getDest();
            src=msg.getSrc();
            if(src == null) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.UDP_NIO_SENDERS_ADDRESS_IS_NULL);
            }
            else {
                ((LogicalAddress)src).setPrimaryPhysicalAddress(sender);
            }

            // discard my own multicast loopback copy
            if((dst == null || dst.isMulticastAddress()) && src != null && local_addr.equals(src)) {
                if(trace)
                    log.trace("discarded own loopback multicast packet");

                // System.out.println("-- discarded " + msg.getObject());

                return;
            }

            evt=new Event(Event.MSG, msg);
            if(trace)
                log.trace("Message is " + msg + ", headers are " + msg.getHeaders());

            /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
             * This allows e.g. PerfObserver to get the time of reception of a message */
            if(observer != null)
                observer.up(evt, up_queue.size());

            hdr=(UdpHeader)msg.removeHeader(name);
        } 
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.UDP_NIO_EXCEPTION_0, Util.getStackTrace(e));
            return;
        }

        if(hdr != null) {

            /* Discard all messages destined for a channel with a different name */
            String ch_name=null;

            if(hdr.channel_name != null)
                ch_name=hdr.channel_name;

            // Discard if message's group name is not the same as our group name unless the
            // message is a diagnosis message (special group name DIAG_GROUP)
            // GemStoneAddition: now we support a different group name by
            // transparently adjusting serialization and semantics for older
            // versions till the min specified in Version.OLDEST_P2P_SUPPORTED
            if(ch_name != null && group_name != null && !group_name.equals(ch_name) &&
                    !ch_name.equals(Util.DIAG_GROUP)) {
                    if(warn) log.warn("discarded message from different group (" +
                            ch_name + "). Sender was " + msg.getSrc());
                return;
            }
        }

        passUp(evt);
    }


    /**
     * Send a message to the address specified in dest
     */
    void sendUdpMessage(Message msg) throws Exception {
        Address            dest, src;
        ObjectOutputStream out;
        byte               buf[];
        DatagramPacket     packet;
        Message            copy;
        Event              evt; // for loopback messages

        dest=msg.getDest();  // if non-null: unicast, else multicast
        src=msg.getSrc();
        if(src == null) {
            src=local_addr_canonical; // no physical addresses present
            msg.setSrc(src);
        }

        if(trace)
            log.trace("sending message to " + msg.getDest() +
                    " (src=" + msg.getSrc() + "), headers are " + msg.getHeaders());

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        if(dest == null || dest.isMulticastAddress() || dest.equals(local_addr)) {
            copy=msg.copy();
            copy.removeHeader(name);
            evt=new Event(Event.MSG, copy);

            /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
               This allows e.g. PerfObserver to get the time of reception of a message */
            if(observer != null)
                observer.up(evt, up_queue.size());
            if(trace) log.trace("looped back local message " + copy);

            // System.out.println("\n-- passing up packet id=" + copy.getObject());
            passUp(evt);
            // System.out.println("-- passed up packet id=" + copy.getObject());

            if(dest != null && !dest.isMulticastAddress())
                return; // it is a unicast message to myself, no need to put on the network
        }

        out_stream.reset();
        out=new ObjectOutputStream(out_stream);
        out.writeShort(JGroupsVersion.version);
        msg.writeExternal(out);
        out.flush(); // needed if out buffers its output to out_stream
        buf=out_stream.toByteArray();
        packet=new DatagramPacket(buf, buf.length, mcast_addr);

        //System.out.println("-- sleeping 4 secs");
        // Thread.sleep(4000);


        // System.out.println("\n-- sending packet " + msg.getObject());
        ct.send(packet);
        // System.out.println("-- sent " + msg.getObject());
    }


    void sendMultipleUdpMessages(Message msg, Vector dests) {
        Address dest;

        for(int i=0; i < dests.size(); i++) {
            dest=(Address)dests.elementAt(i);
            msg.setDest(dest);

            try {
                sendUdpMessage(msg);
            }
            catch(Exception e) {
                if(log.isDebugEnabled()) log.debug("exception=" + e);
            }
        }
    }





//
//    /**
//     * Workaround for the problem encountered in certains JDKs that a thread listening on a socket
//     * cannot be interrupted. Therefore we just send a dummy datagram packet so that the thread 'wakes up'
//     * and realizes it has to terminate. Should be removed when all JDKs support Thread.interrupt() on
//     * reads. Uses send_sock t send dummy packet, so this socket has to be still alive.
//     *
//     * @param dest The destination host. Will be local host if null
//     * @param port The destination port
//     */
//    void sendDummyPacket(InetAddress dest, int port) {
//        DatagramPacket packet;
//        byte[] buf={0};
//
//        if(dest == null) {
//            try {
//                dest=InetAddress.getLocalHost();
//            } catch(Exception e) {
//            }
//        }
//
//        if(trace) if(log.isInfoEnabled()) log.info("UDP_NIO.sendDummyPacket()", "sending packet to " + dest + ":" + port);
//
//        if(ucast_sock == null || dest == null) {
//            if(warn) log.warn("UDP_NIO.sendDummyPacket()", "send_sock was null or dest was null, cannot send dummy packet");
//            return;
//        }
//        packet=new DatagramPacket(buf, buf.length, dest, port);
//        try {
//            ucast_sock.send(packet);
//        } catch(Throwable e) {
//            if(log.isErrorEnabled()) log.error("UDP_NIO.sendDummyPacket()", "exception sending dummy packet to " +
//                    dest + ":" + port + ": " + e);
//        }
//    }





    void handleDownEvent(Event evt) {
        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                synchronized(members) {
                    members.removeAllElements();
                    Vector tmpvec=((View)evt.getArg()).getMembers();
                    for(int i=0; i < tmpvec.size(); i++)
                        members.addElement(tmpvec.elementAt(i));
                }
                break;

            case Event.GET_LOCAL_ADDRESS:   // return local address -> Event(SET_LOCAL_ADDRESS, local)
                passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
                break;

            case Event.CONNECT:
                group_name=(String)evt.getArg();
                udp_hdr=new UdpHeader(group_name);

                // removed March 18 2003 (bela), not needed (handled by GMS)
                // changed July 2 2003 (bela): we discard CONNECT_OK at the GMS level anyway, this might
                // be needed if we run without GMS though
                passUp(new Event(Event.CONNECT_OK));
                break;

            case Event.DISCONNECT:
                passUp(new Event(Event.DISCONNECT_OK));
                break;

            case Event.CONFIG:
                 if(log.isInfoEnabled()) log.info(ExternalStrings.UDP_NIO_RECEIVED_CONFIG_EVENT__0, evt.getArg());
                handleConfigEvent((HashMap)evt.getArg());
                break;
        }
    }


    void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("additional_data"))
            additional_data=(byte[])map.get("additional_data");
        if(map.containsKey("send_buf_size")) {
            mcast_send_buf_size=((Integer)map.get("send_buf_size")).intValue();
            ucast_send_buf_size=mcast_send_buf_size;
        }
        if(map.containsKey("recv_buf_size")) {
            mcast_recv_buf_size=((Integer)map.get("recv_buf_size")).intValue();
            ucast_recv_buf_size=mcast_recv_buf_size;
        }
    }


    /** Return the first non-loopback interface */
    public String determineDefaultBindInterface() throws SocketException {
        for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
            NetworkInterface ni=(NetworkInterface)en.nextElement();
            for(Enumeration en2=ni.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress bind_addr=(InetAddress)en2.nextElement();
                if(!bind_addr.isLoopbackAddress()) {
                    return bind_addr.getHostAddress();
                }
            }
        }
        return null;
    }

    public List determineAllBindInterfaces() throws SocketException {
        List ret=new ArrayList();
        for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
            NetworkInterface ni=(NetworkInterface)en.nextElement();
            for(Enumeration en2=ni.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress bind_addr=(InetAddress)en2.nextElement();
                ret.add(bind_addr.getHostAddress());
            }
        }

        return ret;
    }

    /* ----------------------------- End of Private Methods ---------------------------------------- */



    /* ----------------------------- Inner Classes ---------------------------------------- */


    /**
     * This thread fetches byte buffers from the packet_queue, converts them into messages and passes them up
     * to the higher layer (done in handleIncomingUdpPacket()).
     */
    class PacketHandler implements Runnable {
        Thread t=null;

        public void run() {
            byte[] data;
            SocketAddress sender;

            while(packet_queue != null && packet_handler != null) {
                try {
                    Object[] arr=(Object[])packet_queue.remove();
                    data=(byte[])arr[0];
                    sender=(SocketAddress)arr[1];
                } catch (InterruptedException ie) { // GemStoneAddition
                     if(log.isInfoEnabled()) log.info(ExternalStrings.UDP_NIO_PACKET_HANDLER_THREAD_TERMINATING);
                    break; // exit loop and thread
                } catch(QueueClosedException closed_ex) {
                     if(log.isInfoEnabled()) log.info(ExternalStrings.UDP_NIO_PACKET_HANDLER_THREAD_TERMINATING);
                    break;
                }
                handleIncomingUdpPacket(data, sender);
                data=null; // let's give the poor garbage collector a hand...
            }
        }

        void start() {
            if(t == null) {
                t=new Thread(this, "UDP_NIO.PacketHandler thread");
                t.setDaemon(true);
                t.start();
            }
        }

        void stop() {
            if(packet_queue != null)
                packet_queue.close(false); // should terminate the packet_handler thread too
            t=null;
            packet_queue=null;
        }
    }




    /**
     * Manages a multicast and unicast socket on a given interface (NIC). The multicast socket is used
     * to listen for incoming multicast packets, the unicast socket is used to (1) listen for incoming
     * unicast packets, (2) to send unicast packets and (3) to send multicast packets
     */
    public static class Connector implements Runnable {

        protected Thread t=null; // GemStoneAddition -- accesses synchronized on this

        protected SenderThread sender_thread=null;

        /** Interface on which ucast_sock and mcast_sender_sock are created */
        NetworkInterface bind_interface;


        /** Used for sending/receiving unicast/multicast packets. The reason we have to use a MulticastSocket versus a
         * DatagramSocket is that only MulticastSockets allow to set the interface over which a multicast
         * is sent: DatagramSockets consult the routing table to find the interface
         */
        MulticastSocket mcast_sock=null;

        /** Local port of the mcast_sock */
        SocketAddress localAddr=null;

        /** The receiver which handles incoming packets */
        Receiver receiver=null;

        /** Buffer for incoming unicast packets */
        protected byte[] receive_buffer=null;


        Queue send_queue=new Queue();

        static final GemFireTracer mylog=GemFireTracer.getLog(Connector.class);
        static final boolean mywarn=mylog.isWarnEnabled();


        class SenderThread extends Thread  {


          @Override // GemStoneAddition  
            public void run() {
                Object[] arr;
                byte[] buf;
                SocketAddress dest;

                while(send_queue != null) {
                    try {
                        arr=(Object[])send_queue.remove();
                        buf=(byte[])arr[0];
                        dest=(SocketAddress)arr[1];
                        mcast_sock.send(new DatagramPacket(buf, buf.length, dest));
                    }
                    catch(QueueClosedException e) {
                        break;
                    }
                    catch (InterruptedException ie) { // GemStoneAddition
                        break; // exit loop and thread
                    }
                    catch(SocketException e) {
                        e.printStackTrace();
                    }
                    catch(IOException e) {
                        e.printStackTrace();
                    }

                }
            }
        }



        public Connector(NetworkInterface bind_interface, int local_bind_port,
                         int port_range,  int receive_buffer_size,
                         int receive_sock_buf_size, int send_sock_buf_size,
                         int ip_ttl, Receiver receiver) throws IOException {
            this.bind_interface=bind_interface;
            this.receiver=receiver;
            this.receive_buffer=new byte[receive_buffer_size];

            mcast_sock=createMulticastSocket(local_bind_port, port_range);

            // changed Bela Dec 31 2003: if loopback is disabled other members on the same machine won't be able
            // to receive our multicasts
            // mcast_sock.setLoopbackMode(true); // we don't want to receive our own multicasts
            mcast_sock.setReceiveBufferSize(receive_sock_buf_size);
            mcast_sock.setSendBufferSize(send_sock_buf_size);
            mcast_sock.setTimeToLive(ip_ttl);
            System.out.println("ttl=" + mcast_sock.getTimeToLive());
            mcast_sock.setNetworkInterface(this.bind_interface); // for outgoing multicasts
            localAddr=mcast_sock.getLocalSocketAddress();
            System.out.println("-- local_addr=" + localAddr);
            System.out.println("-- mcast_sock: send_bufsize=" + mcast_sock.getSendBufferSize() +
                    ", recv_bufsize=" + mcast_sock.getReceiveBufferSize());
        }


        public SocketAddress getLocalAddress() {
            return localAddr;
        }

        public NetworkInterface getBindInterface() {
            return bind_interface;
        }

        public void start() throws Exception {
            if(mcast_sock == null)
                throw new Exception("UDP_NIO.Connector.start(): connector has been stopped (start() cannot be called)");

            synchronized (this) { // GemStoneAddition
            if(t != null && t.isAlive()) {
                if(mywarn) mylog.warn("connector thread is already running");
                return;
            }
            t=new Thread(this, "ConnectorThread for " + localAddr);
            }
            
            t.setDaemon(true);
            t.start();

            sender_thread=new SenderThread();
            sender_thread.start();
        }

        /** Stops the connector. After this call, start() cannot be called, but a new connector has to
         * be created
         */
        public void stop() {
          // GemStoneAddition -- interrupt thread first before shutting
          // down its socket, so that it will know that shutdown is occurring.
            synchronized (this) { // GemStoneAddition
              if (t != null) { // GemStoneAddition
                t.interrupt();
              }
              t = null;
            }
            if(mcast_sock != null)
              mcast_sock.close(); // terminates the thread if running
            mcast_sock=null;
        }



        /** Sends a message using mcast_sock */
        public void send(DatagramPacket packet) throws Exception {
            //mcast_sock.send(packet);

            byte[] buf=packet.getData().clone();
            Object[] arr=new Object[]{buf, packet.getSocketAddress()};
            send_queue.add(arr);
        }

        public void run() {
            DatagramPacket packet=new DatagramPacket(receive_buffer, receive_buffer.length);
            for (;;) { // GemStoneAddition remove variable anti-pattern
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                try {
                    packet.setData(receive_buffer, 0, receive_buffer.length);
                    ConnectorTable.receivePacket(packet, mcast_sock, receiver);
                }
                catch(Exception th) {
                  if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
//                    if(th == null || mcast_sock == null || mcast_sock.isClosed())
//                        break;
                    if(mylog.isErrorEnabled()) mylog.error(ExternalStrings.UDP_NIO__0__EXCEPTION_1, new Object[] {localAddr, th});
                    try { // GemStoneAddition
                    Util.sleep(300); // so we don't get into 100% cpu spinning (should NEVER happen !)
                    }
                    catch (InterruptedException e) {
                      break; // exit loop and thread
                    }
                }
            }
            t=null;
        }




        @Override // GemStoneAddition  
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append("local_addr=").append(localAddr).append(", mcast_group=");
            return sb.toString();
        }





        // 27-6-2003 bgooren, find available port in range (start_port, start_port+port_range)
        private MulticastSocket createMulticastSocket(int local_bind_port, int port_range) throws IOException {
            MulticastSocket sock=null;
            int             tmp_port=local_bind_port;

            int max_port=tmp_port + port_range;
            while(tmp_port <= max_port) {
                try {
                    sock=new MulticastSocket(tmp_port);
                    break;
                }
                catch(Exception bind_ex) {
                    tmp_port++;
                }
            }
            if(sock == null)
                throw new IOException("could not create a MulticastSocket (port range: " + local_bind_port +
                        " - " + (local_bind_port+port_range));
            return sock;
        }
    }





    /** Manages a bunch of Connectors */
    public static class ConnectorTable implements Receiver, Runnable {

        Thread t=null; // GemStoneAddition -- accesses synchronized on this

        /** Socket to receive multicast packets. Will be bound to n interfaces */
        MulticastSocket mcast_sock=null;

        /** The multicast address which mcast_sock will join (e.g. 230.1.2.3:7500) */
        InetSocketAddress mcastAddr=null;

        Receiver receiver=null;

        /** Buffer for incoming packets */
        byte[] receive_buffer=null;

        /** Vector<Connector>. A list of Connectors, one for each interface we listen on */
        Vector connectors=new Vector();

//        boolean running=false; GemStoneAddition non-volatile part of a coding anti-pattern

        static final GemFireTracer mylog=GemFireTracer.getLog(ConnectorTable.class);
        static final boolean mywarn=mylog.isWarnEnabled();




        public ConnectorTable(InetSocketAddress mcast_addr,
                              int receive_buffer_size, int receive_sock_buf_size,
                              boolean ip_mcast, Receiver receiver) throws IOException {
            this.receiver=receiver;
            this.mcastAddr=mcast_addr;
            this.receive_buffer=new byte[receive_buffer_size];

            if(ip_mcast) {
                mcast_sock=new MulticastSocket(mcast_addr.getPort());
                // changed Bela Dec 31 2003: if loopback is disabled other members on the same machine won't be able
                // to receive our multicasts
                // mcast_sock.setLoopbackMode(true); // do not get own multicasts
                mcast_sock.setReceiveBufferSize(receive_sock_buf_size);
            }
        }


        public Receiver getReceiver() {
            return receiver;
        }

        public void setReceiver(Receiver receiver) {
            this.receiver=receiver;
        }


        /** Get all interfaces, create one Connector per interface and call start() on it */
        public void start() throws Exception {
            Connector tmp;
//            if(running) GemStoneAddition
//                return;

            if(mcast_sock != null) {
                // Start the thread servicing the incoming multicasts
              synchronized (this) { // GemStoneAddition
                if (t == null || !t.isAlive()) { // GemStoneAddition
                  t=new Thread(this, "ConnectorTable thread");
                  t.setDaemon(true);
                  t.start();
                }
              }
            }


            // Start all Connectors
            for(Iterator it=connectors.iterator(); it.hasNext();) {
                tmp=(Connector)it.next();
                tmp.start();
            }

//            running=true; GemStoneAddition
        }


        public void stop() {
            Connector tmp;
            for(Iterator it=connectors.iterator(); it.hasNext();) {
                tmp=(Connector)it.next();
                tmp.stop();
            }
            connectors.clear();
            synchronized (this) { // GemStoneAddition
              if (t != null) t.interrupt(); // GemStoneAddition
              t=null;
            }
            if(mcast_sock != null) {
                mcast_sock.close();
//                mcast_sock=null; leave open, avoid NPE GemStoneAddition
            }
//            running=false; GemStoneAddition
        }


        public void run() {
            // receive mcast packets on any interface of the list of interfaces we're listening on
            DatagramPacket p=new DatagramPacket(receive_buffer, receive_buffer.length);
            for (;;) { // GemStoneAddition -- avoid anti-pattern
//              if (mcast_sock.isClosed()) break; // GemStoneAddition - but just let receivePacket fail, it's cheaper
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition -- for safety
                p.setData(receive_buffer, 0, receive_buffer.length);
                try {
                    receivePacket(p, mcast_sock, this);
                }
                catch(Exception th) {
                  if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
//                    if(th == null || mcast_sock == null || mcast_sock.isClosed())
//                        break;
                    if(mylog.isErrorEnabled()) mylog.error(ExternalStrings.UDP_NIO_EXCEPTION_0, th);
                    try { // GemStoneAddition
                      Util.sleep(300); // so we don't get into 100% cpu spinning (should NEVER happen !)
                    }
                    catch (InterruptedException e) {
                      break; // exit loop and thread
                    }
                }
            }
//            t=null; GemStoneAddition
        }


        /**
         * Returns a list of local addresses (one for each Connector)
         * @return List<SocketAddress>
         */
        public List getConnectorAddresses() {
            Connector c;
            ArrayList ret=new ArrayList();
            for(Iterator it=connectors.iterator(); it.hasNext();) {
                c=(Connector)it.next();
                ret.add(c.getLocalAddress());
            }
            return ret;
        }

        /** Sends a packet. If the destination is a multicast address, call send() on all connectors.
         * If destination is not null, send the message using <em>any</em> Connector: if we send a unicast
         * message, it doesn't matter to which interface we are bound; the kernel will choose the correct
         * interface based on the destination and the routing table. Note that the receiver will have the
         * interface which was chosen by the kernel to send the message as the receiver's address, so the
         * correct Connector will receive a possible response.
         * @param msg
         * @throws Exception
         */
        public void send(DatagramPacket msg) throws Exception {
            InetAddress dest;

            if(msg == null)
                return;
            dest=msg.getAddress();
            if(dest == null)
                throw new IOException("UDP_NIO.ConnectorTable.send(): destination address is null");

            if(dest.isMulticastAddress()) {
                // send to all Connectors
                for(int i=0; i < connectors.size(); i++) {
                    ((Connector)connectors.get(i)).send(msg);
                }
            }
            else {
                // send to a random connector
                Connector c=pickRandomConnector(connectors);
                c.send(msg);
            }
        }

        private Connector pickRandomConnector(Vector conns) {
            int size=conns.size();
            int index=((int)(Util.random(size))) -1;
            return (Connector)conns.get(index);
        }

        /**
         * Adds the given interface address to the list of interfaces on which the receiver mcast
         * socket has to listen.
         * Also creates a new Connector. Calling this method twice on the same interface will throw an exception
         * @param bind_interface
         * @param local_port
         * @param port_range
         * @param receive_buffer_size
         * @throws IOException
         */
        public void listenOn(String bind_interface, int local_port, int port_range, 
                             int receive_buffer_size, int receiver_sock_buf_size, int send_sock_buf_size,
                             int ip_ttl, Receiver receiver) throws IOException {
            if(bind_interface == null)
                return;

            NetworkInterface ni=NetworkInterface.getByInetAddress(InetAddress.getByName(bind_interface));
            if(ni == null)
                throw new IOException("UDP_NIO.ConnectorTable.listenOn(): bind interface for " +
                        bind_interface + " not found");

            Connector tmp=findConnector(ni);
            if(tmp != null) {
                if(mywarn) mylog.warn("connector for interface " + bind_interface +
                        " is already present (will be skipped): " + tmp);
                return;
            }

            // 1. join the group on this interface
            if(mcast_sock != null) {
                mcast_sock.joinGroup(mcastAddr, ni);

                    if(mylog.isInfoEnabled()) mylog.info(ExternalStrings.UDP_NIO_JOINING__0__ON_INTERFACE__1, new Object[] {mcastAddr, ni});
            }

            // 2. create a new Connector
            tmp=new Connector(ni, local_port, port_range, receive_buffer_size, receiver_sock_buf_size,
                    send_sock_buf_size, ip_ttl, receiver);
            connectors.add(tmp);
        }

        private Connector findConnector(NetworkInterface ni) {
            for(int i=0; i < connectors.size(); i++) {
                Connector c=(Connector)connectors.elementAt(i);
                if(c.getBindInterface().equals(ni))
                    return c;
            }
            return null;
        }


        public void receive(DatagramPacket packet) {
            if(receiver != null) {
                receiver.receive(packet);
            }
        }


        @Override // GemStoneAddition
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append("*** todo: implement ***");
            return sb.toString();
        }


        public static void receivePacket(DatagramPacket packet, DatagramSocket sock, Receiver receiver) throws IOException {
            int len;

            sock.receive(packet);
            len=packet.getLength();
            if(len == 1 && packet.getData()[0] == 0) {
                if(mylog.isTraceEnabled()) mylog.trace("received dummy packet");
                return;
            }
            if(receiver != null)
                receiver.receive(packet);
        }
    }






    public static class MyReceiver implements Receiver {
        ConnectorTable t=null;

        public MyReceiver() {

        }

        public void setConnectorTable(ConnectorTable t) {
            this.t=t;
        }

        public void receive(DatagramPacket packet) {
            System.out.println("-- received " + packet.getLength() + " bytes from " + packet.getSocketAddress());
            InetAddress sender=packet.getAddress();
            byte[] buf=packet.getData();
            int len=packet.getLength();
            String tmp=new String(buf, 0, len);
            if(len > 4) {
                if(tmp.startsWith("rsp:")) {
                    System.out.println("-- received respose: \"" + tmp + '\"');
                    return;
                }
            }

            byte[] rsp_buf=("rsp: this is a response to " + tmp).getBytes();
            DatagramPacket response=new DatagramPacket(rsp_buf, rsp_buf.length, sender, packet.getPort());

            try {
                t.send(response);
            }
            catch(Exception e) {
                e.printStackTrace();
                System.err.println("MyReceiver: problem sending response to " + sender);
            }
        }
    }



    public static class MulticastReceiver implements Runnable {
//        Unmarshaller m=null; GemStoneAddition
//        DatagramSocket sock=null; // may be DatagramSocket or MulticastSocket GemStoneAddition(omitted)

        public void run() {
            // receives packet from socket
            // calls Unmarshaller.receive()
        }

    }

    public static class Unmarshaller  {
//        Queue q=null; GemStoneAddition

        void receive(byte[] data, SocketAddress sender) {
            // if (q) --> q.add()
            // unserialize and call handleMessage()
        }
    }



    static void help() {
        System.out.println("UDP_NIO [-help] [-bind_addrs <list of interfaces>]");
    }



    public static void main(String[] args) {
        MyReceiver        r=new MyReceiver();
        ConnectorTable    ct;
        String            line;
        InetSocketAddress mcast_addr;
        BufferedReader    in=null;
        DatagramPacket    packet;
        byte[]            send_buf;
        int               receive_buffer_size=65000;
        boolean           ip_mcast=true;

        try {
            mcast_addr=new InetSocketAddress("230.1.2.3", 7500);
            ct=new ConnectorTable(mcast_addr, receive_buffer_size, 120000, ip_mcast, r);
            r.setConnectorTable(ct);
        }
        catch(Exception t) {
            t.printStackTrace();
            return;
        }

        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                continue;
            }
            if("-bind_addrs".equals(args[i])) {
                while(++i < args.length && !args[i].trim().startsWith("-")) {
                    try {
                        ct.listenOn(args[i], 0, 1, receive_buffer_size, 120000, 12000, 32, r);
                    }
                    catch(IOException e) {
                        e.printStackTrace();
                        return;
                    }
                }
            }
        }


        try {
            ct.start(); // starts all Connectors in turn
            in=new BufferedReader(new InputStreamReader(System.in));
            while(true) {
                System.out.print("> "); System.out.flush();
                line=in.readLine();
                if(line.startsWith("quit") || line.startsWith("exit"))
                    break;
                send_buf=line.getBytes();
                packet=new DatagramPacket(send_buf, send_buf.length, mcast_addr);
                ct.send(packet);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
//            if(ct != null) GemStoneAddition (cannot be null)
                ct.stop();
        }
    }




}


interface Receiver {

    /** Called when data has been received on a socket. When the callback returns, the buffer will be
     * reused: therefore, if <code>buf</code> must be processed on a separate thread, it needs to be copied.
     * This method might be called concurrently by multiple threads, so it has to be reentrant
     * @param packet
     */
    void receive(DatagramPacket packet);
}
