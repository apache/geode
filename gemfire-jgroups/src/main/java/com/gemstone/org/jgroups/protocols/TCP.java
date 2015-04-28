/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: TCP.java,v 1.31 2005/09/29 12:24:37 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.SuspectMember;
import com.gemstone.org.jgroups.blocks.ConnectionTable;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.BoundedList;
import com.gemstone.org.jgroups.util.ExternalStrings;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Vector;




/**
 * TCP based protocol. Creates a server socket, which gives us the local address of this group member. For
 * each accept() on the server socket, a new thread is created that listens on the socket.
 * For each outgoing message m, if m.dest is in the ougoing hashtable, the associated socket will be reused
 * to send message, otherwise a new socket is created and put in the hashtable.
 * When a socket connection breaks or a member is removed from the group, the corresponding items in the
 * incoming and outgoing hashtables will be removed as well.<br>
 * This functionality is in ConnectionTable, which isT used by TCP. TCP sends messages using ct.send() and
 * registers with the connection table to receive all incoming messages.
 * @author Bela Ban
 */
public class TCP extends TP implements ConnectionTable.Receiver {
    private ConnectionTable ct=null;
    private InetAddress	    external_addr=null; // the IP address which is broadcast to other group members
    private int             start_port=7800;    // find first available port starting at this port
    private int	            end_port=0;         // maximum port to bind to
    private long            reaper_interval=0;  // time in msecs between connection reaps
    private long            conn_expire_time=0; // max time a conn can be idle before being reaped

    /** List the maintains the currently suspected members. This is used so we don't send too many SUSPECT
     * events up the stack (one per message !)
     */
    final BoundedList      suspected_mbrs=new BoundedList(20);

    /** Should we drop unicast messages to suspected members or not */
    boolean                skip_suspected_members=true;

    /** Use separate send queues for each connection */
    boolean                use_send_queues=true;

    int                    recv_buf_size=150000;
    int                    send_buf_size=150000;
    int                    sock_conn_timeout=2000; // max time in millis for a socket creation in ConnectionTable



    public TCP() {
    }

    @Override // GemStoneAddition
    public String getName() {
        return "TCP";
    }


    public int getOpenConnections()      {return ct.getNumConnections();}
    public InetAddress getBindAddr() {return bind_addr;}
    public void setBindAddr(InetAddress bind_addr) {this.bind_addr=bind_addr;}
    public int getStartPort() {return start_port;}
    public void setStartPort(int start_port) {this.start_port=start_port;}
    public int getEndPort() {return end_port;}
    public void setEndPort(int end_port) {this.end_port=end_port;}
    public long getReaperInterval() {return reaper_interval;}
    public void setReaperInterval(long reaper_interval) {this.reaper_interval=reaper_interval;}
    public long getConnExpireTime() {return conn_expire_time;}
    public void setConnExpireTime(long conn_expire_time) {this.conn_expire_time=conn_expire_time;}
    @Override // GemStoneAddition
    public boolean isLoopback() {return loopback;}
    @Override // GemStoneAddition
    public void setLoopback(boolean loopback) {this.loopback=loopback;}


    public String printConnections()     {return ct.toString();}


    /** Setup the Protocol instance acording to the configuration string */
    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("start_port");
        if(str != null) {
            start_port=Integer.parseInt(str);
            props.remove("start_port");
        }

        str=props.getProperty("end_port");
        if(str != null) {
            end_port=Integer.parseInt(str);
            props.remove("end_port");
        }

        str=props.getProperty("external_addr");
        if(str != null) {
            try {
                external_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
                if(log.isFatalEnabled()) log.fatal("(external_addr): host " + str + " not known");
                return false;
            }
            props.remove("external_addr");
        }

        str=props.getProperty("reaper_interval");
        if(str != null) {
            reaper_interval=Long.parseLong(str);
            props.remove("reaper_interval");
        }

        str=props.getProperty("conn_expire_time");
        if(str != null) {
            conn_expire_time=Long.parseLong(str);
            props.remove("conn_expire_time");
        }

        str=props.getProperty("sock_conn_timeout");
        if(str != null) {
            sock_conn_timeout=Integer.parseInt(str);
            props.remove("sock_conn_timeout");
        }

        str=props.getProperty("recv_buf_size");
        if(str != null) {
            recv_buf_size=Integer.parseInt(str);
            props.remove("recv_buf_size");
        }

        str=props.getProperty("send_buf_size");
        if(str != null) {
            send_buf_size=Integer.parseInt(str);
            props.remove("send_buf_size");
        }

        str=props.getProperty("skip_suspected_members");
        if(str != null) {
            skip_suspected_members=Boolean.valueOf(str).booleanValue();
            props.remove("skip_suspected_members");
        }

        str=props.getProperty("use_send_queues");
        if(str != null) {
            use_send_queues=Boolean.valueOf(str).booleanValue();
            props.remove("use_send_queues");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.TCP_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);
            return false;
        }
        return true;
    }


    @Override // GemStoneAddition
    public void start() throws Exception {
        ct=getConnectionTable(reaper_interval,conn_expire_time,bind_addr,external_addr,start_port,end_port);
        ct.setUseSendQueues(use_send_queues);
        // ct.addConnectionListener(this);
        ct.setReceiveBufferSize(recv_buf_size);
        ct.setSendBufferSize(send_buf_size);
        ct.setSocketConnectionTimeout(sock_conn_timeout);
        local_addr=ct.getLocalAddress();
        if(additional_data != null && local_addr instanceof IpAddress)
            ((IpAddress)local_addr).setAdditionalData(additional_data);
        super.start();
    }

    @Override // GemStoneAddition
    public void stop() {
        ct.stop();
        super.stop();
    }


    @Override // GemStoneAddition
    protected void handleDownEvent(Event evt) {
        super.handleDownEvent(evt);
        if(evt.getType() == Event.VIEW_CHANGE) {
            suspected_mbrs.removeAll();
        }
        else if(evt.getType() == Event.UNSUSPECT) {
            suspected_mbrs.removeElement(evt.getArg());
        }
    }
    

   /**
    * @param reaperInterval
    * @param connExpireTime
    * @param bindAddress
    * @param startPort
    * @throws Exception
    * @return ConnectionTable
    * Sub classes overrides this method to initialize a different version of
    * ConnectionTable.
    */
   protected ConnectionTable getConnectionTable(long reaperInterval, long connExpireTime, InetAddress bindAddress,
                                                InetAddress externalAddress, int startPort, int endPort) throws Exception {
       ConnectionTable cTable;
       if(reaperInterval == 0 && connExpireTime == 0) {
           cTable=new ConnectionTable(this, bindAddress, externalAddress, startPort, endPort);
       }
       else {
           if(reaperInterval == 0) {
               reaperInterval=5000;
               if(warn) log.warn("reaper_interval was 0, set it to " + reaperInterval);
           }
           if(connExpireTime == 0) {
               connExpireTime=1000 * 60 * 5;
               if(warn) log.warn("conn_expire_time was 0, set it to " + connExpireTime);
           }
           cTable=new ConnectionTable(this, bindAddress, externalAddress, startPort, endPort, 
                                      reaperInterval, connExpireTime);
       }
       return cTable;
   }


    /** ConnectionTable.Receiver interface */
    public void receive(Address sender, byte[] data, int offset, int length) {
        super.receive(local_addr, sender, data, offset, length);
    }




    @Override // GemStoneAddition
    public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {
        Address dest;
        Vector mbrs=(Vector)members.clone();
        for(int i=0; i < mbrs.size(); i++) {
            dest=(Address)mbrs.elementAt(i);
            sendToSingleMember(dest, false, data, offset, length);
        }
    }

    @Override // GemStoneAddition
    public void sendToSingleMember(Address dest, boolean isJoinResponse/*temporary change - do not commit*/, byte[] data, int offset, int length) throws Exception {
        if(trace) log.trace("dest=" + dest + " (" + data.length + " bytes)");
        if(skip_suspected_members) {
            if(suspected_mbrs.contains(dest)) {
                if(trace)
                    log.trace("will not send unicast message to " + dest + " as it is currently suspected");
                return;
            }
        }

//        if(dest.equals(local_addr)) {
//            if(!loopback) // if loopback, we discard the message (was already looped back)
//                receive(dest, data, offset, length); // else we loop it back here
//            return;
//        }
        try {
            ct.send(dest, data, offset, length);
        }
        catch(Exception e) {
            if(members.contains(dest)) {
                if(!suspected_mbrs.contains(dest)) {
                    suspected_mbrs.add(dest);
                    passUp(new Event(Event.SUSPECT, new SuspectMember(local_addr, dest))); // GemStoneAddition SuspectMember
                }
            }
        }
    }


    @Override // GemStoneAddition
    public String getInfo() {
        StringBuffer sb=new StringBuffer();
        sb.append("connections: ").append(printConnections()).append("\n");
        return sb.toString();
    }


    @Override // GemStoneAddition
    public void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast) {
        if(multicast)
            msg.setDest(null);
        else
            msg.setDest(dest);
    }

    @Override // GemStoneAddition
    public void postUnmarshallingList(Message msg, Address dest, boolean multicast) {
        postUnmarshalling(msg, dest, null, multicast);
    }


}
