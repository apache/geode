/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.protocols;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Channel;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.oswego.concurrent.BoundedLinkedQueue;
import com.gemstone.org.jgroups.protocols.pbcast.NAKACK.RetransmissionTooLargeException;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.Buffer;
import com.gemstone.org.jgroups.util.ExposedBufferedInputStream;
import com.gemstone.org.jgroups.util.ExposedBufferedOutputStream;
import com.gemstone.org.jgroups.util.ExposedByteArrayInputStream;
import com.gemstone.org.jgroups.util.ExposedByteArrayOutputStream;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.List;
import com.gemstone.org.jgroups.util.QueueClosedException;
import com.gemstone.org.jgroups.util.TimeScheduler;
import com.gemstone.org.jgroups.util.Util;
//import com.gemstone.org.jgroups.util.Queue; // 1.5

/**
 * Generic transport - specific implementations should extend this abstract class.
 * Features which are provided to the subclasses include
 * <ul>
 * <li>version checking
 * <li>marshalling and unmarshalling
 * <li>message bundling (handling single messages, and message lists)
 * <li>incoming packet handler
 * <li>loopback
 * </ul>
 * A subclass has to override
 * <ul>
 * <li>{@link #sendToAllMembers(byte[], int, int)}
 * <li>{@link #sendToSingleMember(com.gemstone.org.jgroups.Address, boolean, byte[], int, int)}
 * <li>{@link #init()}
 * <li>{@link #start()}: subclasses <em>must</em> call super.start() <em>after</em> they initialize themselves
 * (e.g., created their sockets).
 * <li>{@link #stop()}: subclasses <em>must</em> call super.stop() after they deinitialized themselves
 * <li>{@link #destroy()}
 * </ul>
 * The create() or start() method has to create a local address.<br>
 * The receive(Address, Address, byte[], int, int) method must
 * be called by subclasses when a unicast or multicast message has been received.
 * @author Bela Ban
 * @version $Id: TP.java,v 1.53 2005/12/23 17:08:22 belaban Exp $
 */
public abstract class TP extends Protocol  {

    public static final/*GemStoneAddition*/ boolean VERBOSE = Boolean.getBoolean("TP.VERBOSE")
                                  || Boolean.getBoolean("DistributionManager.DEBUG_JAVAGROUPS");

    /** The address (host and port) of this member */
    Address         local_addr=null;

    /** The name of the group to which this member is connected */
    String          channel_name=null;

    /** The interface (NIC) which should be used by this transport */
    InetAddress     bind_addr=null;

    /** Overrides bind_addr and -Dbind.address: let's the OS return the local host address */
//    boolean         use_local_host=false; GemStoneAddition

    /** If true, the transport should use all available interfaces to receive multicast messages
     * @deprecated  Use {@link #receive_on_all_interfaces} instead */
//    boolean         bind_to_all_interfaces=false; GemStoneAddition

    /** If true, the transport should use all available interfaces to receive multicast messages */
    boolean         receive_on_all_interfaces=false;

    /** List of NetworkInterface of interfaces to receive multicasts on. The multicast receive socket will listen
     * on all of these interfaces. This is a comma-separated list of IP addresses or interface names. E.g.
     * "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded; we only bind to an interface once.
     * If this property is set, it override receive_on_all_interfaces.
     */
    java.util.List  receive_interfaces=null;

    /** If true, the transport should use all available interfaces to send multicast messages. This means
     * the same multicast message is sent N times, so use with care */
    boolean         send_on_all_interfaces=false;

    /** List of NetworkInterface of interfaces to send multicasts on. The multicast send socket will send the
     * same multicast message on all of these interfaces. This is a comma-separated list of IP addresses or
     * interface names. E.g. "192.168.5.1,eth1,127.0.0.1". Duplicates are discarded.
     * If this property is set, it override send_on_all_interfaces.
     */
    java.util.List  send_interfaces=null;


    /** The port to which the transport binds. 0 means to bind to any (ephemeral) port */
    int             bind_port=0;
    int				port_range=1; // 27-6-2003 bgooren, Only try one port by default

    /** The members of this group (updated when a member joins or leaves) */
    final Vector    members=new Vector(11);

    View            view=null;

    /** Pre-allocated byte stream. Used for marshalling messages. Will grow as needed */
//    final ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(1024);
//    final ExposedBufferedOutputStream  buf_out_stream=new ExposedBufferedOutputStream(out_stream, 1024);
//    final ExposedDataOutputStream      dos=new ExposedDataOutputStream(buf_out_stream);

    final ExposedByteArrayInputStream  in_stream=new ExposedByteArrayInputStream(new byte[]{'0'});
    final ExposedBufferedInputStream   buf_in_stream=new ExposedBufferedInputStream(in_stream);
    final DataInputStream              dis=new DataInputStream(buf_in_stream);


    /** If true, messages sent to self are treated specially: unicast messages are
     * looped back immediately, multicast messages get a local copy first and -
     * when the real copy arrives - it will be discarded. Useful for Window
     * media (non)sense */
    boolean         loopback=false;


    /** Discard packets with a different version. Usually minor version differences are okay. Setting this property
     * to true means that we expect the exact same version on all incoming packets */
    boolean         discard_incompatible_packets=false;

    /** Sometimes receivers are overloaded (they have to handle de-serialization etc).
     * Packet handler is a separate thread taking care of de-serialization, receiver
     * thread(s) simply put packet in queue and return immediately. Setting this to
     * true adds one more thread */
    boolean         use_incoming_packet_handler=false;

    /** Used by packet handler to store incoming DatagramPackets */
    com.gemstone.org.jgroups.util.Queue           incoming_packet_queue=null;

    /** Dequeues DatagramPackets from packet_queue, unmarshalls them and
     * calls <tt>handleIncomingUdpPacket()</tt> */
    IncomingPacketHandler   incoming_packet_handler=null;


    /** Used by packet handler to store incoming Messages */
    com.gemstone.org.jgroups.util.Queue                  incoming_msg_queue=null;

    IncomingMessageHandler incoming_msg_handler;


    /** Packets to be sent are stored in outgoing_queue and sent by a separate thread. Enabling this
     * value uses an additional thread */
    boolean               use_outgoing_packet_handler=false;

    /** Used by packet handler to store outgoing DatagramPackets */
    BoundedLinkedQueue    outgoing_queue=null;

    /** max number of elements in the bounded outgoing_queue */
    int                   outgoing_queue_max_size=2000;

    OutgoingPacketHandler outgoing_packet_handler=null;

    /** If set it will be added to <tt>local_addr</tt>. Used to implement
     * for example transport independent addresses */
    byte[]          additional_data=null;

    /** Maximum number of bytes for messages to be queued until they are sent. This value needs to be smaller
        than the largest datagram packet size in case of UDP */
    int max_bundle_size=AUTOCONF.senseMaxFragSizeStatic();

    /** Max number of milliseconds until queued messages are sent. Messages are sent when max_bundle_size or
     * max_bundle_timeout has been exceeded (whichever occurs faster)
     */
    long max_bundle_timeout=20;

    /** Enabled bundling of smaller messages into bigger ones */
    boolean enable_bundling=false;

    Bundler            bundler=null;

    TimeScheduler      timer=null;

    DiagnosticsHandler diag_handler=null;
    boolean enable_diagnostics=true;
    String diagnostics_addr="224.0.0.75";
    int    diagnostics_port=7500;


    /** HashMap key=Address, value=Address. Keys=senders, values=destinations. For each incoming message M with sender S, adds
     * an entry with key=S and value= sender's IP address and port.
     */
//    HashMap addr_translation_table=new HashMap(); GemStoneAddition

//    boolean use_addr_translation=false; GemStoneAddition

    TpHeader header;

    final String name=getName();
    
    static final byte LIST      = 1;  // we have a list of messages rather than a single message when set
    static final byte MULTICAST = 2;  // message is a multicast (versus a unicast) message when set

    long num_msgs_sent=0, num_msgs_received=0, num_bytes_sent=0, num_bytes_received=0;

    transient static  NumberFormat f;
    
//    private boolean connected; // GemStoneAddition - set to true once connected

    /** GemStoneAddition - ping/pong is used to see if GemFire is listening on a port */
    private boolean pongReceived;

    private final Object pongSync = new Object();
    
    /** GemStoneAddition - version to use when multicasting.  This defaults
     * to 1.0.0 to allow multicast discovery to work during a rolling upgrade.
     */
    private short multicastVersion = Message.multicastVersion;

    static {
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }


    /**
     * Just ensure that this class gets loaded.
     * 
     * @see SystemFailure#loadEmergencyClasses()
     */
    public static void loadEmergencyClasses() {
      // just using java.net, java.lang; we're safe.
    }
    
    /**
     * Closes the diagnostic handler, if it is open
     * 
     * @see SystemFailure#emergencyClose()
     */
    public void emergencyClose() {
      DiagnosticsHandler ds = diag_handler;
      if (ds != null) {
        MulticastSocket ms = ds.diag_sock;
        if (ms != null) {
          ms.close();
        }
        Thread thr = ds.t;
        if (thr != null) {
          thr.interrupt();
        }
      }
    }
    
    /**
     * Creates the TP protocol, and initializes the
     * state variables, does however not start any sockets or threads.
     */
    protected TP() {
    }

    /**
     * debug only
     */
    @Override // GemStoneAddition  
    public String toString() {
        return name + "(local address: " + local_addr + ')';
    }

    @Override // GemStoneAddition  
    public void resetStats() {
        num_msgs_sent=num_msgs_received=num_bytes_sent=num_bytes_received=0;
    }

    public long getNumMessagesSent()     {return num_msgs_sent;}
    public long getNumMessagesReceived() {return num_msgs_received;}
    public long getNumBytesSent()        {return num_bytes_sent;}
    public long getNumBytesReceived()    {return num_bytes_received;}
    public String getBindAddress() {return bind_addr != null? bind_addr.toString() : "null";}
    public InetAddress getInetBindAddress() { return bind_addr; } // GemStoneAddition
    public void setBindAddress(String bind_addr) throws UnknownHostException {
        this.bind_addr=InetAddress.getByName(bind_addr);
    }
    /** @deprecated Use {@link #isReceiveOnAllInterfaces()} instead */
    @Deprecated // GemStoneAddition
    public boolean getBindToAllInterfaces() {return receive_on_all_interfaces;}
    public void setBindToAllInterfaces(boolean flag) {this.receive_on_all_interfaces=flag;}

    public boolean isReceiveOnAllInterfaces() {return receive_on_all_interfaces;}
    public java.util.List getReceiveInterfaces() {return receive_interfaces;}
    public boolean isSendOnAllInterfaces() {return send_on_all_interfaces;}
    public java.util.List getSendInterfaces() {return send_interfaces;}
    public boolean isDiscardIncompatiblePackets() {return discard_incompatible_packets;}
    public void setDiscardIncompatiblePackets(boolean flag) {discard_incompatible_packets=flag;}
    public boolean isEnableBundling() {return enable_bundling;}
    public void setEnableBundling(boolean flag) {enable_bundling=flag;}
    public int getMaxBundleSize() {return max_bundle_size;}
    public void setMaxBundleSize(int size) {max_bundle_size=size;}
    public long getMaxBundleTimeout() {return max_bundle_timeout;}
    public void setMaxBundleTimeout(long timeout) {max_bundle_timeout=timeout;}
    public int getOutgoingQueueSize() {return outgoing_queue != null? outgoing_queue.size() : 0;}
    public int getIncomingQueueSize() {return incoming_packet_queue != null? incoming_packet_queue.size() : 0;}
    public Address getLocalAddress() {return local_addr;}
    public String getChannelName() {return channel_name;}
    public boolean isLoopback() {return loopback;}
    public void setLoopback(boolean b) {loopback=b;}
    public boolean isUseIncomingPacketHandler() {return use_incoming_packet_handler;}
    public boolean isUseOutgoingPacketHandler() {return use_outgoing_packet_handler;}
    public int getOutgoingQueueMaxSize() {return outgoing_queue != null? outgoing_queue_max_size : 0;}
    public void setOutgoingQueueMaxSize(int new_size) {
        if(outgoing_queue != null) {
            outgoing_queue.setCapacity(new_size);
            outgoing_queue_max_size=new_size;
        }
    }


    @Override // GemStoneAddition  
    public Map dumpStats() {
        Map retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap();
        retval.put("num_msgs_sent", Long.valueOf(num_msgs_sent));
        retval.put("num_msgs_received", Long.valueOf(num_msgs_received));
        retval.put("num_bytes_sent", Long.valueOf(num_bytes_sent));
        retval.put("num_bytes_received", Long.valueOf(num_bytes_received));
        return retval;
    }


    /**
     * Send to all members in the group. UDP would use an IP multicast message, whereas TCP would send N
     * messages, one for each member
     * @param data The data to be sent. This is not a copy, so don't modify it
     * @param offset
     * @param length
     * @throws Exception
     */
    public abstract void sendToAllMembers(byte[] data, int offset, int length) throws Exception;

    /**
     * Send to all members in the group. UDP would use an IP multicast message, whereas TCP would send N
     * messages, one for each member
     * @param dest Must be a non-null unicast address
     * @param data The data to be sent. This is not a copy, so don't modify it
     * @param offset
     * @param length
     * @throws Exception
     */
    public abstract void sendToSingleMember(Address dest, boolean isJoinResponse, byte[] data, int offset, int length) throws Exception;

    public abstract String getInfo();

    public abstract void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast);

    public abstract void postUnmarshallingList(Message msg, Address dest, boolean multicast);


    private String _getInfo() {
        StringBuffer sb=new StringBuffer();
        sb.append(local_addr).append(" (").append(channel_name).append(") ").append("\n");
        sb.append("local_addr=").append(local_addr).append("\n");
        sb.append("group_name=").append(channel_name).append("\n");
        sb.append("Version=").append(JGroupsVersion.description).append(", cvs=\"").append(JGroupsVersion.cvs).append("\"\n");
        sb.append("view: ").append(view).append('\n');
        sb.append(getInfo());
        return sb.toString();
    }


    protected/*GemStoneAddition*/ void handleDiagnosticProbe(SocketAddress sender, DatagramSocket sock, String request) {
        try {
            StringTokenizer tok=new StringTokenizer(request);
            String req=tok.nextToken();
            String info="n/a";
            if(req.trim().toLowerCase().startsWith("query")) {
                ArrayList l=new ArrayList(tok.countTokens());
                while(tok.hasMoreTokens())
                    l.add(tok.nextToken().trim().toLowerCase());

                info=_getInfo();

                if(l.contains("jmx")) {
                    if(info == null) info="";
                    Channel ch=stack.getChannel();
                    if(ch != null) {
                        Map m=ch.dumpStats();
                        StringBuffer sb=new StringBuffer();
                        sb.append("stats:\n");
                        for(Iterator it=m.entrySet().iterator(); it.hasNext();) {
                            sb.append(it.next()).append("\n");
                        }
                        info+=sb.toString();
                    }
                }
                if(l.contains("props")) {
                    String p=stack.printProtocolSpecAsXML();
                    info+="\nprops:\n" + p;
                }
            }


            byte[] diag_rsp=info.getBytes();
            if(log.isDebugEnabled())
                log.debug("sending diag response to " + sender);
            sendResponse(sock, sender, diag_rsp);
        }
        catch (VirtualMachineError err) { // GemStoneAddition
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch(Throwable t) {
            if(log.isErrorEnabled())
                log.error(ExternalStrings.TP_FAILED_SENDING_DIAG_RSP_TO__0, sender, t);
        }
    }
    
    /** GemStoneAddition - AvailablePort support.  Send a response to a ping from
        another process that's groping for an available multicast port */
    private void sendPingResponse(Address asender) {
      byte msgbuf[] = new byte[4];
      msgbuf[0] = (byte)'p';
      msgbuf[1] = (byte)'o';
      msgbuf[2] = (byte)'n';
      msgbuf[3] = (byte)'g';
      IpAddress sender = (IpAddress)asender;
      sender.setName(""); // The address will hold this VM's default attributes, so get rid of the name field before logging it
      if (log.isDebugEnabled()) {
        log.debug("Responding to ping-pong request from " + sender);
      }
      try {
        doSend(new Buffer(msgbuf, 0, msgbuf.length), false, sender, false);
      }
      catch (Exception e) {
        if (log.getLogWriter().fineEnabled()) {
          log.getLogWriter().fine("exception sending ping response to " + sender, e);
        }
      }
    }

    private void sendResponse(DatagramSocket sock, SocketAddress sender, byte[] buf) throws IOException {
        DatagramPacket p=new DatagramPacket(buf, 0, buf.length, sender);
        sock.send(p);
    }

    /* ------------------------------------------------------------------------------- */



    /*------------------------------ Protocol interface ------------------------------ */


    /**
     * Creates the unicast and multicast sockets and starts the unicast and multicast receiver threads
     */
    @Override // GemStoneAddition  
    public void start() throws Exception {
        timer=stack.timer;
        if(timer == null)
            throw new Exception("timer is null");

        if(enable_diagnostics) {
            diag_handler=new DiagnosticsHandler();
            diag_handler.start();
        }

        if(use_incoming_packet_handler) {
            incoming_packet_queue=new com.gemstone.org.jgroups.util.Queue();
            incoming_packet_handler=new IncomingPacketHandler();
            incoming_packet_handler.start();
        }

        if(loopback) {
            incoming_msg_queue=new com.gemstone.org.jgroups.util.Queue();
            incoming_msg_handler=new IncomingMessageHandler();
            incoming_msg_handler.start();
        }

        if(use_outgoing_packet_handler) {
            outgoing_queue=new BoundedLinkedQueue(outgoing_queue_max_size);
            outgoing_packet_handler=new OutgoingPacketHandler();
            outgoing_packet_handler.start();
        }

        if(enable_bundling) {
            bundler=new Bundler();
        }
        
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }


    @Override // GemStoneAddition  
    public void stop() {
        if(diag_handler != null) {
            diag_handler.stop();
            diag_handler=null;
        }

        // 1. Stop the outgoing packet handler thread
        if(outgoing_packet_handler != null)
            outgoing_packet_handler.stop();


        // 2. Stop the incoming packet handler thread
        if(incoming_packet_handler != null)
            incoming_packet_handler.stop();


        // 3. Finally stop the incoming message handler
        if(incoming_msg_handler != null)
            incoming_msg_handler.stop();
    }



    /**
     * Setup the Protocol instance according to the configuration string
     * @return true if no other properties are left.
     *         false if the properties still have data in them, ie ,
     *         properties are left over and not handled by the protocol stack
     */
    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String str;
        String tmp = null;

        super.setProperties(props);

        // PropertyPermission not granted if running in an untrusted environment with JNLP.
        try {
            tmp=System.getProperty("bind.address");
            if(Util.isBindAddressPropertyIgnored()) {
                tmp=null;
            }
        }
        catch (SecurityException ex){
        }

        if(tmp != null)
            str=tmp;
        else
            str=props.getProperty("bind_addr");
        if(str != null) {
            try {
                bind_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
                if(log.isFatalEnabled()) log.fatal("(bind_addr): host " + str + " not known");
                return false;
            }
            props.remove("bind_addr");
        }

        str=props.getProperty("use_local_host");
        if(str != null) {
//            use_local_host=Boolean.valueOf/*GemStoneAddition*/(str).booleanValue(); GemStoneAddition(omitted)
            props.remove("use_local_host");
        }

        str=props.getProperty("bind_to_all_interfaces");
        if(str != null) {
            receive_on_all_interfaces=Boolean.valueOf/*GemStoneAddition*/(str).booleanValue();
            props.remove("bind_to_all_interfaces");
            log.warn("bind_to_all_interfaces has been deprecated; use receive_on_all_interfaces instead");
        }

        str=props.getProperty("receive_on_all_interfaces");
        if(str != null) {
            receive_on_all_interfaces=Boolean.valueOf/*GemStoneAddition*/(str).booleanValue();
            props.remove("receive_on_all_interfaces");
        }

        str=props.getProperty("receive_interfaces");
        if(str != null) {
            try {
                receive_interfaces=parseInterfaceList(str);
                props.remove("receive_interfaces");
            }
            catch(Exception e) {
                log.error(ExternalStrings.TP_ERROR_DETERMINING_INTERFACES__0_, str, e);
                return false;
            }
        }

        str=props.getProperty("send_on_all_interfaces");
        if(str != null) {
            send_on_all_interfaces=Boolean.valueOf/*GemStoneAddition*/(str).booleanValue();
            props.remove("send_on_all_interfaces");
        }

        str=props.getProperty("send_interfaces");
        if(str != null) {
            try {
                send_interfaces=parseInterfaceList(str);
                props.remove("send_interfaces");
            }
            catch(Exception e) {
                log.error(ExternalStrings.TP_ERROR_DETERMINING_INTERFACES__0_, str, e);
                return false;
            }
        }

        str=props.getProperty("bind_port");
        if(str != null) {
            bind_port=Integer.parseInt(str);
            props.remove("bind_port");
        }

        str=props.getProperty("port_range");
        if(str != null) {
            port_range=Integer.parseInt(str);
            props.remove("port_range");
        }

        str=props.getProperty("loopback");
        if(str != null) {
            loopback=Boolean.valueOf(str).booleanValue();
            props.remove("loopback");
        }

        str=props.getProperty("discard_incompatible_packets");
        if(str != null) {
            discard_incompatible_packets=Boolean.valueOf(str).booleanValue();
            props.remove("discard_incompatible_packets");
        }

        // this is deprecated, just left for compatibility (use use_incoming_packet_handler)
        str=props.getProperty("use_packet_handler");
        if(str != null) {
            use_incoming_packet_handler=Boolean.valueOf(str).booleanValue();
            props.remove("use_packet_handler");
            if(warn) log.warn("'use_packet_handler' is deprecated; use 'use_incoming_packet_handler' instead");
        }

        str=props.getProperty("use_incoming_packet_handler");
        if(str != null) {
            use_incoming_packet_handler=Boolean.valueOf(str).booleanValue();
            props.remove("use_incoming_packet_handler");
        }

        str=props.getProperty("use_outgoing_packet_handler");
        if(str != null) {
            use_outgoing_packet_handler=Boolean.valueOf(str).booleanValue();
            props.remove("use_outgoing_packet_handler");
        }

        str=props.getProperty("outgoing_queue_max_size");
        if(str != null) {
            outgoing_queue_max_size=Integer.parseInt(str);
            props.remove("outgoing_queue_max_size");
            if(outgoing_queue_max_size <= 0) {
                if(log.isWarnEnabled())
                    log.warn("outgoing_queue_max_size of " + outgoing_queue_max_size + " is invalid, setting it to 1");
                outgoing_queue_max_size=1;
            }
        }

        str=props.getProperty("max_bundle_size");
        if(str != null) {
            int bundle_size=Integer.parseInt(str);
            if(bundle_size > max_bundle_size) {
                // GemStoneAddition - don't prevent stack from starting if this happens
                if(log.isWarnEnabled()) log.warn("auto sensed max datagram size (" + bundle_size +
                        ") is greater than udp_fragment_size setting plus overhead (" + max_bundle_size + ')');
                //return false;
            }
            if(bundle_size <= 0) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.TP_MAX_BUNDLE_SIZE__0__IS__0, bundle_size);
                return false;
            }
            max_bundle_size=bundle_size;
            props.remove("max_bundle_size");
        }

        str=props.getProperty("max_bundle_timeout");
        if(str != null) {
            max_bundle_timeout=Long.parseLong(str);
            if(max_bundle_timeout <= 0) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.TP_MAX_BUNDLE_TIMEOUT_OF__0__IS_INVALID, max_bundle_timeout);
                return false;
            }
            props.remove("max_bundle_timeout");
        }

        str=props.getProperty("enable_bundling");
        if(str != null) {
            enable_bundling=Boolean.valueOf(str).booleanValue();
            props.remove("enable_bundling");
        }

        str=props.getProperty("use_addr_translation");
        if(str != null) {
//            use_addr_translation=Boolean.valueOf(str).booleanValue(); GemStoneAddition
            props.remove("use_addr_translation");
        }

        str=props.getProperty("enable_diagnostics");
        if(str != null) {
            enable_diagnostics=Boolean.valueOf(str).booleanValue();
            props.remove("enable_diagnostics");
        }

        str=props.getProperty("diagnostics_addr");
        if(str != null) {
            diagnostics_addr=str;
            props.remove("diagnostics_addr");
        }

        str=props.getProperty("diagnostics_port");
        if(str != null) {
            diagnostics_port=Integer.parseInt(str);
            props.remove("diagnostics_port");
        }

        if(enable_bundling) {
            //if (use_outgoing_packet_handler == false)
              //  if(warn) log.warn("enable_bundling is true; setting use_outgoing_packet_handler=true");
            // use_outgoing_packet_handler=true;
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
     * @param evt - the event being send from the stack
     */
    @Override // GemStoneAddition  
    public void up(Event evt) {
        switch(evt.getType()) {
        case Event.CONFIG:
            passUp(evt);
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((HashMap)evt.getArg());
            return;
        }
        passUp(evt);
    }
    
    /**
     * Caller by the layer above this layer. Usually we just put this Message
     * into the send queue and let one or more worker threads handle it. A worker thread
     * then removes the Message from the send queue, performs a conversion and adds the
     * modified Message to the send queue of the layer below it, by calling down()).
     */
    @Override // GemStoneAddition  
    public void down(Event evt) {
        
        if(evt.getType() != Event.MSG) {  // unless it is a message handle it and respond
            handleDownEvent(evt);
            return;
        }

        // GemStoneAddition - do not send messages if this thread has been interrupted.
        // It is quite possible that the message content is corrupt
        if (Thread.currentThread().isInterrupted()) {
          return;
        }

        Message msg=(Message)evt.getArg();
        if(header != null) {
            // added patch by Roland Kurmann (March 20 2003)
            // msg.putHeader(name, new TpHeader(channel_name));
            msg.putHeader(name, header);
        }

        // Because we don't call Protocol.passDown(), we notify the observer directly (e.g. PerfObserver).
        // This way, we still have performance numbers for TP
        if(observer != null)
            observer.passDown(evt);

        setSourceAddress(msg); // very important !! listToBuffer() will fail with a null src address !!
        if (msg.bundleable && msg.isHighPriority) {
          msg.bundleable = false;
        }
        if (VERBOSE || GemFireTracer.DEBUG) {
            StringBuffer sb=new StringBuffer("sending msg ");
            sb.append(msg.toString());
            if (!msg.isHighPriority && enable_bundling && msg.bundleable) {
              sb.append(" bundled");
            }
            log.getLogWriter().info(ExternalStrings.DEBUG, sb);
        }

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        Address dest=msg.getDest();
        boolean multicast=dest == null || dest.isMulticastAddress();
        if(loopback && (multicast || dest.equals(local_addr))) {
            Message copy=msg.copy(true);

            // copy.removeHeader(name); // we don't remove the header
            copy.setSrc(local_addr);
            // copy.setDest(dest);

            if(trace) log.trace("looping back message"); // GemStoneAddition - don't log message again
            try {
                incoming_msg_queue.add(copy);
                stack.gfPeerFunctions.setJgQueuedMessagesSize(incoming_msg_queue.size());
            }
            catch(QueueClosedException e) {
//                 if (trace) log.error("failed adding looped back message to incoming_msg_queue", e);
            }

            if(!multicast)
                return;
        }

        try {
            if(use_outgoing_packet_handler)
                outgoing_queue.put(msg);
            else
                send(msg, dest, multicast);
        }
        catch(QueueClosedException closed_ex) {
        }
        catch(InterruptedException interruptedEx) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        catch (VirtualMachineError err) { // GemStoneAddition
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch (RetransmissionTooLargeException e) {
          throw e;
        }
        catch(Throwable e) {
          // GemStoneAddition - look for out-of-buffer-space condition
          if ( (e instanceof Exception) && (e.getCause() != null) &&
                "No buffer space available".equals(e.getCause().getMessage())) {
            log.getLogWriter().warning(
                ExternalStrings.TP_OUT_OF_SOCKET_BUFFER_SPACE_INCREASE_0,
                (multicast? " mcast-send-buffer-size"
                            : " udp-send-buffer-size"));
          }
           //if(log.isErrorEnabled() &&
            if (!Thread.currentThread().isInterrupted()  // GemStoneAddition - don't log interrupts
              ) {
              if (VERBOSE || GemFireTracer.DEBUG) {
                log.getLogWriter().fine("failed sending message", e);
              }
            }
        }
    }



    /*--------------------------- End of Protocol interface -------------------------- */


    /* ------------------------------ Private Methods -------------------------------- */



    /**
     * If the sender is null, set our own address. We cannot just go ahead and set the address
     * anyway, as we might be sending a message on behalf of someone else ! E.gin case of
     * retransmission, when the original sender has crashed, or in a FLUSH protocol when we
     * have to return all unstable messages with the FLUSH_OK response.
     */
    private void setSourceAddress(Message msg) {
        if(msg.getSrc() == null)
            msg.setSrc(local_addr);
    }
    
    /** GemStoneAddition
     * this sends a multicast message and waits for the given period of time
     * for a response
     * @param timeout
     * @return true if a response was received
     */
    public boolean testMulticast(long timeout) {
      byte[] buffer = new byte[4];
      buffer[0] = (byte)'p';
      buffer[1] = (byte)'i';
      buffer[2] = (byte)'n';
      buffer[3] = (byte)'g';
      this.pongReceived = false;
      try {
        this.sendToAllMembers(buffer, 0, 4);
      } catch (Exception e) {
        if (log.getLogWriter().fineEnabled()) {
          log.getLogWriter().fine("exception sending ping request", e);
        }
        return false;
      }
      long waitEnd = System.currentTimeMillis() + timeout;
      long remainingMs = timeout;
      synchronized(this.pongSync ) {
        try {
          while (!this.pongReceived && remainingMs > 0) {
            this.pongSync.wait(remainingMs);
            remainingMs = waitEnd - System.currentTimeMillis();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return this.pongReceived;
      }
    }

    /**
     * Subclasses must call this method when a unicast or multicast message has been received.
     * Declared final so subclasses cannot override this method.
     *
     * @param dest
     * @param sender
     * @param data
     * @param offset
     * @param length
     */
    protected final void receive(Address dest, Address sender, byte[] data, int offset, int length) {
        if(data == null) return;

//        if(length == 4) {  // received a diagnostics probe
//            if(data[offset] == 'd' && data[offset+1] == 'i' && data[offset+2] == 'a' && data[offset+3] == 'g') {
//                handleDiagnosticProbe(sender);
//                return;
//            }
//        }
        // GemStoneAddition - record time until msg reaches jgmm
        long start = 0;
        if (stack != null && stack.enableClockStats)
          start = nanoTime();
        boolean mcast=dest == null || dest.isMulticastAddress();
//        if(trace){
//            StringBuffer sb=new StringBuffer("received (");
//            sb.append(mcast? "mcast)" : "ucast) ").append(length).append(" bytes from ").append(sender);
//            log.trace(sb.toString());
//        }

        // GemStoneAddition - AvailablePort support
        if (length == 4) {
          if (data[offset] == 'p' && data[offset+1] == 'i' && data[offset+2] == 'n' && data[offset+3] == 'g') {
            sendPingResponse(sender);
            return;
          } else if (data[offset] == 'p' && data[offset+1] == 'o' && data[offset+2] == 'n' && data[offset+3] == 'g') {
            synchronized(this.pongSync) {
              this.pongReceived = true;
              this.pongSync.notifyAll();
            }
            this.stack.gfPeerFunctions.pongReceived(
                  ((IpAddress)sender).getSocketAddress());
            return;
          }
        }
             
        try {
            if(use_incoming_packet_handler) {
                byte[] tmp=new byte[length];
                System.arraycopy(data, offset, tmp, 0, length);
                incoming_packet_queue.add(new IncomingQueueEntry(dest, sender, tmp, offset, length, start)); // GemStoneAddition - pass along start time
            }
            else
                handleIncomingPacket(dest, sender, data, offset, length, start); // GemStoneAddition - pass along start time
        }
        catch (VirtualMachineError err) { // GemStoneAddition
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch(Throwable t) {
            if(log.isErrorEnabled())
                log.error(ExternalStrings.TP_FAILED_HANDLING_DATA_FROM_0, sender, t);
        }
    }


    /**
     * Processes a packet read from either the multicast or unicast socket. Needs to be synchronized because
     * mcast or unicast socket reads can be concurrent.
     * Correction (bela April 19 2005): we access no instance variables, all vars are allocated on the stack, so
     * this method should be reentrant: removed 'synchronized' keyword
     */
    protected/*GemStoneAddition*/ void handleIncomingPacket(Address dest, Address sender, byte[] data, int offset, int length,
      long startTime /* GemStoneAddition */) {
        Message                msg=null;
        List                   l=null;  // used if bundling is enabled
        short                  version;
        boolean                is_message_list, multicast;
        byte                   flags;

        try {
//          System.out.println("handleIncomingPacket("+dest+", "+sender+","+data.length+","+offset+")");
            synchronized(in_stream) {
                in_stream.setData(data, offset, length);
                buf_in_stream.reset(length);
                version=dis.readShort();
                if(JGroupsVersion.compareTo(version) == false) {
                    if(warn) {
                        StringBuffer sb=new StringBuffer();
                        sb.append("packet from ").append(sender).append(" has different version (").append(version);
                        sb.append(") from ours (").append(JGroupsVersion.printVersion()).append("). ");
                        if(discard_incompatible_packets)
                            sb.append("Packet is discarded");
                        else
                            sb.append("This may cause problems");
                        log.warn(sb);
                    }
                    if(discard_incompatible_packets)
                        return;
                }

                flags=dis.readByte();
                is_message_list=(flags & LIST) == LIST;
                multicast=(flags & MULTICAST) == MULTICAST;
                
                if(is_message_list) {
                    l=bufferToList(dis, dest, sender, multicast);
                    if (l == null) {
                      return;
                    }
                }
                else {
                    msg=bufferToMessage(dis, dest, sender, multicast);
                    if (msg == null) {
                      return;
                    }
                    msg.timeStamp = startTime; // GemStoneAddition
                }
            }

            LinkedList msgs=new LinkedList();
            if(is_message_list) {
                for(Enumeration en=l.elements(); en.hasMoreElements();) {
                    Message m = (Message)en.nextElement(); // GemStoneAddition
                    // we use current time instead of startTime
                    // so that passUp() with no up-threads
                    // doesn't artificially inflate the channel time for
                    // bundled messages
                    if (stack.enableClockStats)
                      m.timeStamp = nanoTime();
                    msgs.add(m);
                }
            }
            else
                msgs.add(msg);

            Address src;
            for(Iterator it=msgs.iterator(); it.hasNext();) {
                msg=(Message)it.next();
                src=msg.getSrc();
                // GemStoneAddition - this loop was very poorly written & has been recoded
                if (loopback
                    && multicast && src != null && local_addr.equals(src)) {
                  if (trace) {
                    log.getLogWriter().info(ExternalStrings.DEBUG, "discarding my own multicast message " + msg);
                  }
                  it.remove();
                  continue;
                }
                else {
//                if (incoming_msg_queue == null || msg.isHighPriority) {
                  handleIncomingMessage(msg);
                  it.remove();
//                }
                }
            }
            // GemStoneAddition - we want to use loopback but not queue all
            // messages coming over the wire.  Non-loopback messages should
            // just be dispatched
//            if(incoming_msg_queue != null && msgs.size() > 0) {
//                incoming_msg_queue.addAll(msgs);
//                if (stack.gemfireStats != null) {
//                  stack.gemfireStats.setJgQueuedMessagesSize(incoming_msg_queue.size());
//                }
//            }
        }
        catch(QueueClosedException closed_ex) {
            ; // swallow exception
        }
        catch (VirtualMachineError err) { // GemStoneAddition
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch(Throwable t) {
//          if(log.isErrorEnabled()) {
              log.getLogWriter().info(ExternalStrings.TP_FAILED_UNMARSHALLING_MESSAGE_FROM__0,
                  new Object[]{sender}, t);
//          }
        }
    }


    protected/*GemStoneAddition*/ void handleIncomingMessage(Message msg) {
        Event      evt;
        TpHeader   hdr;

        if(stats) {
            num_msgs_received++;
            num_bytes_received+=msg.getLength();
        }
        
        evt=new Event(Event.MSG, msg);

        if(VERBOSE || GemFireTracer.DEBUG) {
            // GemStoneAddition - headers are always displayed in Message.toString() now
            StringBuffer sb=new StringBuffer("received message ").append(msg);; // .append(", headers are ").append(msg.getHeaders());
            log.getLogWriter().info(ExternalStrings.DEBUG, sb);
        }

        /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
        * This allows e.g. PerfObserver to get the time of reception of a message */
        if(observer != null)
            observer.up(evt, up_queue.size());

        hdr=(TpHeader)msg.getHeader(name); // replaced removeHeader() with getHeader()
        if(hdr != null) {

            /* Discard all messages destined for a channel with a different name */
            String ch_name=hdr.channel_name;

            // Discard if message's group name is not the same as our group name unless the
            // message is a diagnosis message (special group name DIAG_GROUP)
            // GemStoneAddition: now we support a different group name by
            // transparently adjusting serialization and semantics for older
            // versions till the min specified in Version.OLDEST_P2P_SUPPORTED
            if(ch_name != null && channel_name != null && !channel_name.equals(ch_name) /* &&
                    !ch_name.equals(Util.DIAG_GROUP)*/) {  // GemStoneAddition - remove a string compare
                if(warn)
                    log.warn(new StringBuilder("discarded message from different group \"").append(ch_name).
                            append("\" (our group is \"").append(channel_name).append("\"). Sender was ").append(msg.getSrc()));
                return;
            }
            // GemStoneAddition - remove the TP header to reduce footprint in UNICAST/NAKACK receive window
            msg.removeHeader(name);
        }
        else {
            if(trace)
                log.trace(new StringBuffer("message does not have a transport header, msg is ").append(msg).
                          append(", headers are ").append(msg.getHeaders()).append(", will be discarded"));
            return;
        }
        // gemfire addition.
        long sTime = 0;
        if (stack.enableJgStackStats)
          sTime = nanoTime();
        
        passUp(evt);
        
        // gemfire addition.
        if (stack.enableJgStackStats)
          stack.gfPeerFunctions.incjgUpTime(nanoTime() - sTime);       
    }


    /** Internal method to serialize and send a message. This method is not reentrant */
    protected/*GemStoneAddition*/ void send(Message msg, Address dest, boolean multicast) throws Exception {
        if(enable_bundling) { // GemStoneAddition - bundleable
            if (msg.bundleable && multicast) {
              bundler.send(msg, dest);
              return;
            }
            //else {  [bruce] this is more correct, but has a perf impact.  sending out of order doesn't seem
            //                to cause problems
              // flush any bundled messages so we don't have ordering problems
            //  bundler.bundleAndSend();
            //}
        }

//        if (log.isDebugEnabled()) // GemStoneAddition - for last ditch debugging
//          log.debug("sending message unbundled"); // + msg.toString() + " headers: " + msg.printObjectHeaders());


        // Needs to be synchronized because we can have possible concurrent access, e.g.
        // Discovery uses a separate thread to send out discovery messages
        // We would *not* need to sync between send(), OutgoingPacketHandler and BundlingOutgoingPacketHandler,
        // because only *one* of them is enabled
        Buffer   buf;
//        synchronized(out_stream) {
            buf=messageToBuffer(msg, multicast);
            if (buf.getLength() > 60000) {
              if (com.gemstone.org.jgroups.protocols.pbcast.NAKACK.isRetransmission(msg)) {
                throw new com.gemstone.org.jgroups.protocols.pbcast.NAKACK.RetransmissionTooLargeException("serialized size is " + buf.getLength());
              }
            }
            doSend(buf, msg.isJoinResponse, dest, multicast);
//        }
    }


    protected/*GemStoneAddition*/ void doSend(Buffer buf, boolean isJoinResponse, Address dest, boolean multicast) throws Exception {
        if(stats) {
            num_msgs_sent++;
            num_bytes_sent+=buf.getLength();
        }
        if(multicast) {
            sendToAllMembers(buf.getBuf(), buf.getOffset(), buf.getLength());
        }
        else {
            sendToSingleMember(dest, isJoinResponse, buf.getBuf(), buf.getOffset(), buf.getLength());
        }
    }



    /**
     * This method needs to be synchronized on out_stream when it is called
     * @param msg
     * @return Buffer
     * @throws java.io.IOException
     */
    private Buffer messageToBuffer(Message msg, boolean multicast) throws Exception {
        Buffer retval;
        byte flags=0;

//        out_stream.reset();
//        buf_out_stream.reset(out_stream.getCapacity());
        ExposedByteArrayOutputStream out_stream = new ExposedByteArrayOutputStream((int)msg.size());
        ExposedBufferedOutputStream buf_out_stream = new ExposedBufferedOutputStream(out_stream, (int)msg.size());
        DataOutputStream dos = new DataOutputStream(buf_out_stream);  //dos.reset();
        dos.writeShort(JGroupsVersion.version); // write the version
        short msgVersion = 0;
        if(multicast) {
            flags+=MULTICAST;
            msgVersion = getMulticastVersion();
        }
        dos.writeByte(flags);
        if (multicast) {
          JGroupsVersion.writeOrdinal(dos, msgVersion, true);
        }
        // preMarshalling(msg, dest, src);  // allows for optimization by subclass
        msg.setVersion(msgVersion);
        msg.writeTo(dos);
        // postMarshalling(msg, dest, src); // allows for optimization by subclass
        dos.flush();
        retval=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
        return retval;
    }

    protected Message bufferToMessage(DataInputStream instream, Address dest, Address sender, boolean multicast) throws Exception {
        short mcastVersion = JChannel.getGfFunctions().getCurrentVersionOrdinal();
        if (multicast) {
          short mcastOrdinal = JGroupsVersion.readOrdinal(instream);
          if (mcastOrdinal > mcastVersion) {
            // newer mcast packet than this VM can handle - ignore it
            log.getLogWriter().info(ExternalStrings.IGNORING_MULTICAST_MESSAGE_WITH_HIGHER_VERSION_FROM_0_1_2,
                new Object[]{sender, mcastOrdinal, mcastVersion});
            return null;
          }
          mcastVersion = mcastOrdinal;
        }
        Message msg=new Message(false); // don't create headers, readFrom() will do this
        msg.setVersion(mcastVersion);
        msg.readFrom(instream);
        postUnmarshalling(msg, dest, sender, multicast); // allows for optimization by subclass
        return msg;
    }



    protected/*GemStoneAddition*/ Buffer listToBuffer(List l, boolean multicast) throws Exception {
        Buffer retval;
//        Address src; GemStoneAddition
        Message msg;
        byte flags=0;
        int len=l != null? l.size() : 0;
//        boolean src_written=false; GemStoneAddition

//      out_stream.reset();
//      buf_out_stream.reset(out_stream.getCapacity());
        ExposedByteArrayOutputStream out_stream = new ExposedByteArrayOutputStream(65535);
        ExposedBufferedOutputStream buf_out_stream = new ExposedBufferedOutputStream(out_stream, 65535);

//        dos.reset();
        DataOutputStream dos = new DataOutputStream(buf_out_stream);
        dos.writeShort(JGroupsVersion.version);
        flags+=LIST;
        short mcastVersion = 0; 
        if(multicast) {
            flags+=MULTICAST;
        }
        dos.writeByte(flags);
        if (multicast) {
          mcastVersion = getMulticastVersion();
          // record the version for reading so we know how to deserialize the address
          JGroupsVersion.writeOrdinal(dos, mcastVersion, true);
          dos = stack.gfBasicFunctions.getVersionedDataOutputStream(dos, mcastVersion);
        }
        dos.writeInt(len);
        boolean writeaddrs = true;
        if (local_addr != null) {
          writeaddrs=false;
          dos.writeBoolean(false);
          Util.writeAddress(local_addr, dos);
        }
        else {
          dos.writeBoolean(true);
        }
        if (l != null) { // GemStoneAddition
        for(Enumeration en=l.elements(); en.hasMoreElements();) {
            msg=(Message)en.nextElement();
            //src=msg.getSrc();
            //if(!src_written) {
            //    Util.writeAddress(src, dos);
            //    src_written=true;
            //}
            boolean resurrect = false;
            if (!writeaddrs) {
              if (msg.getSrc().equals(local_addr)) {
                msg.setSrc(null);
                resurrect=true;
              }
            }
            if (multicast) {
              msg.setVersion(mcastVersion);
            }
            msg.writeTo(dos);
            if (resurrect)
              msg.setSrc(local_addr);
        }
        } // GemStoneAddition
        dos.flush();
        retval=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
        return retval;
    }
    
    /**
     * GemStoneAddition.   When mcasting we choose the oldest version in the
     * current membership set for serializing messages.
     */
    private void determineMulticastVersion() {
      short currOrdinal = JChannel.getGfFunctions().getCurrentVersionOrdinal();
      short result = currOrdinal;
      synchronized(this.members) {
        for (Iterator it=this.members.iterator(); it.hasNext(); ) {
          short ver = ((Address)it.next()).getVersionOrdinal();
          if (ver < currOrdinal) {
            if (result == 0) {
              result = ver;
            } else if (ver < result) {
              result = ver;
            }
          }
        }
      }
      this.multicastVersion = result;
      Message.multicastVersion = result;
    }
    
    private short getMulticastVersion() {
      synchronized(this.members) {
        if (this.multicastVersion <= 0) {
          return JChannel.getGfFunctions().getCurrentVersionOrdinal();
        } else {
          return this.multicastVersion;
        }
      }
    }

    private List bufferToList(DataInputStream instream, Address dest, Address source, boolean multicast) throws Exception {  // GemStoneAddition - source parameter
        List                    l=new List();
        DataInputStream         in=null;
        int                     len;
        Message                 msg;
        Address                 src = null;

        short currentVersionOrdinal = stack.gfBasicFunctions.getCurrentVersionOrdinal();
        try {
          if (multicast) {
            short mcastOrdinal = JGroupsVersion.readOrdinal(instream);
            if (mcastOrdinal > currentVersionOrdinal) {
              // newer mcast packet than this VM can handle - ignore it
              log.getLogWriter().info(ExternalStrings.IGNORING_MULTICAST_MESSAGE_WITH_HIGHER_VERSION_FROM_0_1_2,
                  new Object[]{source, mcastOrdinal, currentVersionOrdinal});
              return null;
            }
            if (mcastOrdinal < currentVersionOrdinal) {
              instream = stack.gfBasicFunctions.getVersionedDataInputStream(instream, mcastOrdinal);
            }
          }
            len=instream.readInt();
            boolean readaddr = !instream.readBoolean();
            if (readaddr) {
              src = Util.readAddress(instream);
              // GemStoneAddition - canonical source addresses
              synchronized(this.members) {
                // GemStoneAddition - canonical member IDs
                int idx = this.members.indexOf(src);
                if (idx >= 0) {
                  src = (Address)this.members.get(idx);
                }
              }
            }
            //src=Util.readAddress(instream); GemStoneAddition - bug 34280 - messages contain their source address
            
            for(int i=0; i < len; i++) {
                msg=new Message(false); // don't create headers, readFrom() will do this
                msg.readFrom(instream);
                if (readaddr && (msg.getSrc() == null))
                  msg.setSrc(src);
                postUnmarshallingList(msg, dest, multicast);
                l.add(msg);
            }
            return l;
        }
        finally {
            Util.closeInputStream(in);
        }
    }




    /**
     *
     * @param s
     * @return List of NetworkInterface
     */
    private java.util.List parseInterfaceList(String s) throws Exception {
        java.util.List interfaces=new ArrayList(10);
        if(s == null)
            return null;

        StringTokenizer tok=new StringTokenizer(s, ",");
        String interface_name;
        NetworkInterface intf;

        while(tok.hasMoreTokens()) {
            interface_name=tok.nextToken();

            // try by name first (e.g. (eth0")
            intf=NetworkInterface.getByName(interface_name);

            // next try by IP address or symbolic name
            if(intf == null)
                intf=NetworkInterface.getByInetAddress(InetAddress.getByName(interface_name));

            if(intf == null)
                throw new Exception("interface " + interface_name + " not found");
            if(interfaces.contains(intf)) {
                log.warn("did not add interface " + interface_name + " (already present in " + print(interfaces) + ")");
            }
            else {
                interfaces.add(intf);
            }
        }
        return interfaces;
    }

    private String print(java.util.List interfaces) {
        StringBuffer sb=new StringBuffer();
        boolean first=true;
        NetworkInterface intf;
        for(Iterator it=interfaces.iterator(); it.hasNext();) {
            intf=(NetworkInterface)it.next();
            if(first) {
                first=false;
            }
            else {
                sb.append(", ");
            }
            sb.append(intf.getName());
        }
        return sb.toString();
    }


    protected void handleDownEvent(Event evt) {
        switch(evt.getType()) {

        case Event.TMP_VIEW:
        case Event.VIEW_CHANGE:
            synchronized(members) {
                view=(View)evt.getArg();
                members.clear();
                Vector tmpvec=view.getMembers();
                members.addAll(tmpvec);
                determineMulticastVersion();
            }
            break;

        case Event.GET_LOCAL_ADDRESS:   // return local address -> Event(SET_LOCAL_ADDRESS, local)
            passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
            break;

        case Event.CONNECT:
            channel_name=(String)evt.getArg();
            header=new TpHeader(channel_name);

            // removed March 18 2003 (bela), not needed (handled by GMS)
            // changed July 2 2003 (bela): we discard CONNECT_OK at the GMS level anyway, this might
            // be needed if we run without GMS though
            passUp(new Event(Event.CONNECT_OK));
            break;
            
        case Event.CONNECT_OK: // GemStoneAddition - connect_ok handling
//          this.connected = true;
          break;

        case Event.DISCONNECT:
            passUp(new Event(Event.DISCONNECT_OK));
            break;

        case Event.CONFIG:
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((HashMap)evt.getArg());
            break;
        }
    }


    protected void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("additional_data"))
            additional_data=(byte[])map.get("additional_data");
    }



    /* ----------------------------- End of Private Methods ---------------------------------------- */



    /* ----------------------------- Inner Classes ---------------------------------------- */

    static/*GemStoneAddition*/ class IncomingQueueEntry {
        Address   dest=null;
        Address   sender=null;
        byte[]    buf;
        int       offset, length;
        long      timeStamp; // GemStoneAddition

        IncomingQueueEntry(Address dest, Address sender, byte[] buf, int offset, int length, long timeStamp /* GemStoneAddition */) {
            this.dest=dest;
            this.sender=sender;
            this.buf=buf;
            this.offset=offset;
            this.length=length;
            this.timeStamp = timeStamp; // GemStoneAddition
        }
    }




    /**
     * This thread fetches byte buffers from the packet_queue, converts them into messages and passes them up
     * to the higher layer (done in handleIncomingUdpPacket()).
     */
    class IncomingPacketHandler implements Runnable {
      // GemStoneAddition #t must be synchronized on this
        Thread t=null;

        synchronized /* GemStoneAddition */ void start() {
            if(t == null || !t.isAlive()) {
                t=new Thread(this, "UDP Incoming Packet Handler"); // GemStoneAddition - prefix with "UDP" for DM.isPreciousThread()
                t.setDaemon(true);
                t.start();
            }
        }

        synchronized /* GemStoneAddition */ void stop() {
            incoming_packet_queue.close(true); // should terminate the packet_handler thread too
            if (t != null)
              t.interrupt(); // GemStoneAddition
            t=null;
        }

        public void run() {
            IncomingQueueEntry entry;
            for (;;) { // GemStoneAddition - remove coding anti-pattern
              if (incoming_packet_queue.closed()) break; // GemStoneAddition
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition -- for safety
                try {
                    entry=(IncomingQueueEntry)incoming_packet_queue.remove();
                    handleIncomingPacket(entry.dest, entry.sender, entry.buf, entry.offset, entry.length,
                      entry.timeStamp); // GemStoneAddition - timestamp
                }
                // GemStoneAddition - handle interrupts by exiting
                catch (InterruptedException ie) {
                  if (log.isTraceEnabled()) log.trace("packet handler thread terminating (interrupted)");
//                  Thread.currentThread().interrupt(); not really necessary...
                  break;
                }
                catch(QueueClosedException closed_ex) {
                    break;
                }
                catch (VirtualMachineError err) { // GemStoneAddition
                  // If this ever returns, rethrow the error.  We're poisoned
                  // now, so don't let this thread continue.
                  throw err;
                }
                catch(Throwable ex) {
                    if(log.isErrorEnabled())
                        log.error(ExternalStrings.TP_ERROR_PROCESSING_INCOMING_PACKET, ex);
                }
            }
            if(trace) log.trace("incoming packet handler terminating");
        }
    }


    class IncomingMessageHandler implements Runnable {
        Thread t; // GemStoneAddition -- accesses synchronized on this
//        int i=0; GemStoneAddition

        synchronized /* GemStoneAddition */ public void start() {
            if(t == null || !t.isAlive()) {
                t=new Thread(this, "UDP Loopback Message Handler");
                t.setDaemon(true);
                t.setPriority(Thread.MAX_PRIORITY);
                t.start();
            }
        }


        synchronized /* GemStoneAddition */ public void stop() {
          // GemStoneAddition - bug #40633, OOME during shutdown due to this queue
          // not draining
            incoming_msg_queue.close(false); 
            if (t != null) t.interrupt(); // GemStoneAddition
            t=null;
        }

        public void run() {
            Message msg;
            for (;;) { // GemStoneAddition - avoid coding anti-pattern
              if (incoming_msg_queue.closed()) break; // GemStoneAddition
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                try {
                    msg=(Message)incoming_msg_queue.remove();
//                    if (trace) log.trace("removed message from loopback queue: " + msg);
                    handleIncomingMessage(msg);
                }
                catch (InterruptedException ie) { // GemStoneAddition
                    break; // exit loop and thread
                }
                catch(QueueClosedException closed_ex) {
                    break;
                }
                catch (VirtualMachineError err) { // GemStoneAddition
                  // If this ever returns, rethrow the error.  We're poisoned
                  // now, so don't let this thread continue.
                  throw err;
                }
                catch(Throwable ex) {
                    if(log.isErrorEnabled())
                        log.error(ExternalStrings.TP_ERROR_PROCESSING_INCOMING_MESSAGE, ex);
                }
            }
            if(trace) log.trace("incoming message handler terminating");
        }
    }


    /**
     * This thread fetches byte buffers from the outgoing_packet_queue, converts them into messages and sends them
     * using the unicast or multicast socket
     */
    class OutgoingPacketHandler implements Runnable {
        // GemStoneAddition #t must be synchronized on this
        Thread             t=null;
//        byte[]             buf; GemStoneAddition
//        DatagramPacket     packet; GemStoneAddition

        synchronized /* GemStoneAddition */ void start() {
            if(t == null || !t.isAlive()) {
                t=new Thread(this, "OutgoingPacketHandler");
                t.setDaemon(true);
                t.start();
            }
        }

        synchronized /* GemStoneAddition*/ void stop() {
            Thread tmp=t;
            t=null;
            if(tmp != null) {
                tmp.interrupt();
            }
        }

        public void run() {
            Message msg;
            boolean multicast = false; // GemStoneAddition

            for (;;) { // GemStoneAddition remove anti-pattern
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                try {
                    msg=(Message)outgoing_queue.take();
                    multicast = msg.getDest() == null || msg.getDest().isMulticastAddress(); // GemStoneAddition
                    handleMessage(msg);
                }
                catch(QueueClosedException closed_ex) {
                    break;
                }
                catch(InterruptedException interruptedEx) {
                  break; // GemStoneAddition; exit loop and thread
                }
                catch (VirtualMachineError err) { // GemStoneAddition
                  // If this ever returns, rethrow the error.  We're poisoned
                  // now, so don't let this thread continue.
                  throw err;
                }
                catch(Throwable th) {
                  // GemStoneAddition - look for out-of-buffer-space condition
                  if ( (th instanceof Exception) && (th.getCause() != null) &&
                       "No buffer space available".equals(th.getCause().getMessage())) {
                    log.getLogWriter().warning(
                        ExternalStrings.TP_OUT_OF_SOCKET_BUFFER_SPACE_INCREASE_0,
                        (multicast? " mcast-send-buffer-size"
                                    : " udp-send-buffer-size"));
                  }
                  else {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.TP_EXCEPTION_SENDING_PACKET, th);
                  }
                }
                msg=null; // let's give the garbage collector a hand... this is probably useless though
            }
            if(trace) log.trace("outgoing message handler terminating");
        }

        protected void handleMessage(Message msg) throws Throwable {
            Address dest=msg.getDest();
            send(msg, dest, dest == null || dest.isMulticastAddress());
        }


    }


    protected/*GemStoneAddition*/ class Bundler {
        /** HashMap key=Address, value=List of Message. Keys are destinations, values are lists of Messages */
        final HashMap       msgs=new HashMap(36);
        long                count=0;    // current number of bytes accumulated
        int                 num_msgs=0;
        long                start=0;
        BundlingTimer       bundling_timer=null;


        protected/*GemStoneAddition*/ synchronized void send(Message msg, Address dest) throws Exception {
          long gsstart = 0;
          if (stack.enableClockStats)
            gsstart = nanoTime();  // GemStoneAddition - statistics
          try {
            long length=msg.size();

            //if (trace)
            //  log.trace("bundling " + msg.toString() + " headers: " + msg.printObjectHeaders());

            //if (length > max_bundle_size) { // GemStone - for debugging
            //  msg.dumpPayload();
            //  log.trace("bundler: bad message size " +  msg);
            //}
            if (checkLength(length)) { // GemStoneAddition - dump msg if too long
               log.getLogWriter().warning(ExternalStrings.TP_SENDING_OVERSIZED_MESSAGE__0__HEADERS__1, new Object[] {msg.toString(), msg.printObjectHeaders()});
               log.getLogWriter().warning(ExternalStrings.TP_PAYLOAD___0, msg.toStringAsObject());
            }

            //if(start == 0)
            //    start=System.currentTimeMillis();  // GemStoneAddition - this isn't used

            if(count + length >= max_bundle_size) {
                cancelTimer();
                bundleAndSend();  // clears msgs and resets num_msgs
            }

            addMessage(msg, dest);
            count+=length;
            startTimer(); // start timer if not running
          }
          finally { // GemStoneAddition
            if (stack.enableClockStats) {
              if (log.isDebugEnabled())  // GemStoneAddition - debugging
                log.debug("bundled " + msg.toString() + " headers: " + msg.printObjectHeaders());
              if (stack.enableClockStats)
                stack.gfPeerFunctions.incBatchSendTime(gsstart);
            }
          }
        }

        /** Never called concurrently with cancelTimer - no need for synchronization */
        private void startTimer() {
            if(bundling_timer == null || bundling_timer.cancelled()) {
                bundling_timer=new BundlingTimer();
                timer.add(bundling_timer);
            }
        }

        /** Never called concurrently with startTimer() - no need for synchronization */
        private void cancelTimer() {
            if(bundling_timer != null) {
                bundling_timer.cancel();
                bundling_timer=null;
            }
        }

        private void addMessage(Message msg, Address dest) { // no sync needed, never called by multiple threads concurrently
            List    tmp;
            synchronized(msgs) {
                tmp=(List)msgs.get(dest);
                if(tmp == null) {
                    tmp=new List();
                    msgs.put(dest, tmp);
                }
                tmp.add(msg);
                num_msgs++;
            }
        }


        public void bundleAndSend() { // GemStoneAddition - was private
            Map.Entry      entry;
            Address        dst;
            Buffer         buffer;
            List           l;

            synchronized(msgs) {
                if(msgs.size() == 0)
                    return;

                try {
                    if(trace) {
                        long stop=System.currentTimeMillis();
                        double percentage=100.0 / max_bundle_size * count;
                        StringBuffer sb=new StringBuffer("sending ").append(num_msgs).append(" msgs (");
                        sb.append(count).append(" bytes (" + f.format(percentage) + "% of max_bundle_size), collected in "+
                                + (stop-start) + "ms) to ").append(msgs.size()).
                                append(" destination(s)");
                        if(msgs.size() > 1) sb.append(" (dests=").append(msgs.keySet()).append(")");
                        log.trace(sb.toString());
                    }
                    boolean multicast;
                    for(Iterator it=msgs.entrySet().iterator(); it.hasNext();) {
                        entry=(Map.Entry)it.next();
                        l=(List)entry.getValue();
                        if(l.size() == 0)
                            continue;
                        dst=(Address)entry.getKey();
                        multicast=dst == null || dst.isMulticastAddress();
//                        synchronized(out_stream) {
                            try {
                                 // GemStoneAddition - stats
//                                long start = 0;
                                if (stack.enableClockStats)
                                  start = nanoTime();
                                buffer=listToBuffer(l, multicast);
                                if (stack.enableClockStats) {
                                  stack.gfPeerFunctions.incBatchCopyTime(start);
                                  start = nanoTime();
                                }
                                doSend(buffer, false, dst, multicast);
                                if (stack.enableClockStats)
                                  stack.gfPeerFunctions.incBatchFlushTime(start);
                            }
                            catch (VirtualMachineError err) { // GemStoneAddition
                              // If this ever returns, rethrow the error.  We're poisoned
                              // now, so don't let this thread continue.
                              throw err;
                            }
                            catch(Throwable e) {
                              // GemStoneAddition - look for out-of-buffer-space condition
                              if ( (e instanceof Exception) && (e.getCause() != null) &&
                                   "No buffer space available".equals(e.getCause().getMessage())) {
                                log.getLogWriter().warning(
                                    ExternalStrings.TP_OUT_OF_SOCKET_BUFFER_SPACE_INCREASE_0,
                                    (multicast? " mcast-send-buffer-size"
                                                : " udp-send-buffer-size"));
                              }
                              else {
                                if(log.isErrorEnabled()) log.error(ExternalStrings.TP_EXCEPTION_SENDING_MSG, e);
                              }
                            }
//                        }
                    }
                }
                finally {
                    msgs.clear();
                    num_msgs=0;
                    start=0;
                    count=0;
                }
            }
        }

        private boolean checkLength(long len) throws Exception {  // GemStoneAddition - don't throw exceptions
            return (len > max_bundle_size);
            //    throw new Exception("message size (" + len + ") is greater than max bundling size (" + max_bundle_size +
            //            "). Set the fragmentation/bundle size in FRAG and TP correctly");
        }

        protected/*GemStoneAddition*/ class BundlingTimer implements TimeScheduler.Task {
            boolean cancelled=false;

            void cancel() {
                cancelled=true;
            }

            public boolean cancelled() {
                return cancelled;
            }

            public long nextInterval() {
                return max_bundle_timeout;
            }

            public void run() {
                bundleAndSend();
                cancelled=true;
            }
        }
    }



    private class DiagnosticsHandler implements Runnable {
        /**
         * GemStoneAddition: volatile reads OK, updates synchronized on this.
         */
        volatile Thread t=null;
        /**
         * GemStoneAddition: volatile reads OK, updates synchronized on this.
         */
        volatile MulticastSocket diag_sock=null;

        DiagnosticsHandler() {
        }

        synchronized /* GemStoneAddition */ void start() throws IOException {
            diag_sock=new MulticastSocket(diagnostics_port);
            java.util.List interfaces=Util.getAllAvailableInterfaces();
            bindToInterfaces(interfaces, diag_sock);

            if(t == null || !t.isAlive()) {
                t=new Thread(this, "DiagnosticsHandler");
                t.setDaemon(true);
                t.start();
            }
        }

        synchronized /* GemStoneAddition */ void stop() {
            if(diag_sock != null)
                diag_sock.close();
            if (t != null) t.interrupt();
            t=null;
        }

        public void run() {
            byte[] buf=new byte[1500]; // MTU on most LANs
            DatagramPacket packet;
            for (;;) { // GemStoneAddition - avoid coding anti-pattern
              if (diag_sock.isClosed()) break; // GemStoneAddition
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                packet=new DatagramPacket(buf, 0, buf.length);
                try {
                    diag_sock.receive(packet);
                    handleDiagnosticProbe(packet.getSocketAddress(), diag_sock,
                                          new String(packet.getData(), packet.getOffset(), packet.getLength()));
                }
                catch(IOException e) {
                }
            }
        }

        private void bindToInterfaces(java.util.List interfaces, MulticastSocket s) throws IOException {
            SocketAddress group_addr=new InetSocketAddress(diagnostics_addr, diagnostics_port);
            for(Iterator it=interfaces.iterator(); it.hasNext();) {
                NetworkInterface i=(NetworkInterface)it.next();
                try {
                    s.joinGroup(group_addr, i);
                    if(trace)
                        log.trace("joined " + group_addr + " on " + i.getName());
                }
                catch(IOException e) {
                    log.warn("failed to join " + group_addr + " on " + i.getName() + ": " + e);
                }
            }
        }
    }


}
