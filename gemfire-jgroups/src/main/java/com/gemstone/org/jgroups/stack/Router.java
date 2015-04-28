/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Router.java,v 1.7 2005/05/30 16:14:44 belaban Exp $

package com.gemstone.org.jgroups.stack;


import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.List;
import com.gemstone.org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;




/**
 * Router for TCP based group comunication (using layer TCP instead of UDP). Instead of the TCP
 * layer sending packets point-to-point to each other member, it sends the packet to the router
 * which - depending on the target address - multicasts or unicasts it to the group / or single
 *  member.<p>
 * This class is especially interesting for applets which cannot directly make connections
 * (neither UDP nor TCP) to a host different from the one they were loaded from. Therefore,
 * an applet would create a normal channel plus protocol stack, but the bottom layer would have
 * to be the TCP layer which sends all packets point-to-point (over a TCP connection) to the
 * router, which in turn forwards them to their end location(s) (also over TCP). A centralized
 * router would therefore have to be running on the host the applet was loaded from.<p>
 * An alternative for running JGroups in an applet (IP multicast is not allows in applets as of
 * 1.2), is to use point-to-point UDP communication via the gossip server. However, then the appplet
 * has to be signed which involves additional administrative effort on the part of the user.
 * @author Bela Ban
 */
public class Router  {
    final Hashtable    groups=new Hashtable();  // groupname - vector of AddressEntry's
    int          port=8080;
    ServerSocket srv_sock=null;
    InetAddress  bind_address;
    protected final GemFireTracer log=GemFireTracer.getLog(getClass());

    public static final int GET=-10;
    public static final int REGISTER=-11;
    public static final int DUMP=-21;


    public Router(int port) throws Exception {
        this.port=port;
        srv_sock=new ServerSocket(port, 50);  // backlog of 50 connections
    }

    public Router(int port, InetAddress bind_address) throws Exception {
        this.port=port;
        this.bind_address=bind_address;
        srv_sock=new ServerSocket(port, 50, bind_address);  // backlog of 50 connections
    }


    public void start() {
        Socket sock;
        DataInputStream input;
        DataOutputStream output;
        Address peer_addr;
        byte[] buf;
        int len, type;
        String gname;
        Date d;

        if(bind_address == null) bind_address=srv_sock.getInetAddress();
        d=new Date();
         {
            if(log.isInfoEnabled()) log.info(ExternalStrings.Router_ROUTER_STARTED_AT__0, d);
            if(log.isInfoEnabled()) log.info(ExternalStrings.Router_LISTENING_ON_PORT_0_BOUND_ON_ADDRESS_1, new Object[] {Integer.valueOf(port), bind_address});
        }
        d=null;

        while(true) {
            try {
                sock=srv_sock.accept();
                sock.setSoLinger(true, 500);
                peer_addr=new com.gemstone.org.jgroups.stack.IpAddress(sock.getInetAddress(), sock.getPort());
                output=new DataOutputStream(sock.getOutputStream());

                // return the address of the peer so it can set it
                buf=Util.objectToByteBuffer(peer_addr);
                output.writeInt(buf.length);
                output.write(buf, 0, buf.length);


                // We can have 2 kinds of messages at this point: GET requests or REGISTER requests.
                // GET requests are processed right here, REGISTRATION requests cause the spawning of
                // a separate thread handling it (long running thread as it will do the message routing
                // on behalf of that client for the duration of the client's lifetime).

                input=new DataInputStream(sock.getInputStream());
                type=input.readInt();
                gname=input.readUTF();

                switch(type) {
                    case Router.GET:
                        processGetRequest(sock, output, gname); // closes sock after processing
                        break;
                    case Router.DUMP:
                        processDumpRequest(peer_addr, sock, output); // closes sock after processing
                        break;
                    case Router.REGISTER:
                        Address addr;
                        len=input.readInt();
                        buf=new byte[len];
                        input.readFully(buf, 0, buf.length); // read Address
                        addr=(Address)Util.objectFromByteBuffer(buf);
                        addEntry(gname, new AddressEntry(addr, sock, output));
                        new SocketThread(sock, input).start();
                        break;
                    default:
                        if(log.isErrorEnabled()) log.error(ExternalStrings.Router_REQUEST_OF_TYPE__0__NOT_RECOGNIZED, type);
                        continue;
                }
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.Router_EXCEPTION_0, e);
                continue;
            }
        }
    }


    public void stop() {

    }

    /**
     Gets the members of group 'groupname'. Returns them as a List of Addresses.
     */
    void processGetRequest(Socket sock, DataOutputStream output, String groupname) {
        List grpmbrs=(List)groups.get(groupname), ret=null;
        AddressEntry entry;
        byte[] buf;

        if(log.isTraceEnabled()) log.trace("groupname=" + groupname + ", result=" + grpmbrs);

        if(grpmbrs != null && grpmbrs.size() > 0) {
            ret=new List();
            for(Enumeration e=grpmbrs.elements(); e.hasMoreElements();) {
                entry=(AddressEntry)e.nextElement();
                ret.add(entry.addr);
            }
        }
        try {
            if(ret == null || ret.size() == 0) {
                output.writeInt(0);
            }
            else {
                buf=Util.objectToByteBuffer(ret);
                output.writeInt(buf.length);
                output.write(buf, 0, buf.length);
            }
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Router_EXCEPTION_0, e);
        }
        finally {
            try {
                if(output != null)
                    output.close();
                sock.close();
            }
            catch(Exception e) {
            }
        }
    }


    /**
     * Dumps the routing table on the wire, as String.
     **/
    void processDumpRequest(Address peerAddress, Socket sock, DataOutputStream output) {

        if(log.isTraceEnabled()) log.trace("request from " + peerAddress);

        StringBuffer sb=new StringBuffer();
        synchronized(groups) {
            if(groups.size() == 0) {
                sb.append("empty routing table");
            }
            else {
                for(Iterator i=groups.keySet().iterator(); i.hasNext();) {
                    String gname=(String)i.next();
                    sb.append("GROUP: '" + gname + "'\n");
                    List l=(List)groups.get(gname);
                    if(l == null) {
                        sb.append("\tnull list of addresses\n");
                    }
                    else
                        if(l.size() == 0) {
                            sb.append("\tempty list of addresses\n");
                        }
                        else {
                            for(Enumeration e=l.elements(); e.hasMoreElements();) {
                                AddressEntry ae=(AddressEntry)e.nextElement();
                                sb.append('\t');
                                sb.append(ae.toString());
                                sb.append('\n');
                            }
                        }
                }
            }
        }
        try {
            output.writeUTF(sb.toString());
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Router_ERROR_SENDING_THE_ANSWER_BACK_TO_THE_CLIENT__0, e);
        }
        finally {
            try {
                if(output != null) {
                    output.close();
                }
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.Router_ERROR_CLOSING_THE_OUTPUT_STREAM__0, e);
            }
            try {
                sock.close();
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.Router_ERROR_CLOSING_THE_SOCKET__0, e);
            }
        }
    }

    synchronized void route(Address dest, String dest_group, byte[] msg) {

        if(dest == null) { // send to all members in group dest.getChannelName()
            if(dest_group == null) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.Router_BOTH_DEST_ADDRESS_AND_GROUP_ARE_NULL);
                return;
            }
            else {
                sendToAllMembersInGroup(dest_group, msg);
            }
        }
        else {                  // send to destination address
            DataOutputStream out=findSocket(dest);
            if(out != null)
                sendToMember(out, msg);
            else
                if(log.isErrorEnabled()) log.error(ExternalStrings.Router_ROUTING_OF_MESSAGE_TO__0__FAILED_OUTSTREAM_IS_NULL_, dest);
        }
    }


    void addEntry(String groupname, AddressEntry e) {
        List val;
        AddressEntry old_entry;

        // Util.print("addEntry(" + groupname + ", " + e + ")");

        if(groupname == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Router_GROUPNAME_WAS_NULL_NOT_ADDED_);
            return;
        }

        synchronized(groups) {
            val=(List)groups.get(groupname);

            if(val == null) {
                val=new List();
                groups.put(groupname, val);
            }
            if(val.contains(e)) {
                old_entry=(AddressEntry)val.removeElement(e);
                if(old_entry != null)
                    old_entry.destroy();
            }
            val.add(e);
        }
    }


    void removeEntry(Socket sock) {
        List val;
        AddressEntry entry;

        synchronized(groups) {
            for(Enumeration e=groups.keys(); e.hasMoreElements();) {
                val=(List)groups.get(e.nextElement());

                for(Enumeration e2=val.elements(); e2.hasMoreElements();) {
                    entry=(AddressEntry)e2.nextElement();
                    if(entry.sock == sock) {
                        try {
                            entry.sock.close();
                        }
                        catch(Exception ex) {
                        }
                        //Util.print("Removing entry " + entry);
                        val.removeElement(entry);
                        return;
                    }
                }
            }
        }
    }


    void removeEntry(OutputStream out) {
        List val;
        AddressEntry entry;

        synchronized(groups) {
            for(Enumeration e=groups.keys(); e.hasMoreElements();) {
                val=(List)groups.get(e.nextElement());

                for(Enumeration e2=val.elements(); e2.hasMoreElements();) {
                    entry=(AddressEntry)e2.nextElement();
                    if(entry.output == out) {
                        try {
                            if(entry.sock != null)
                                entry.sock.close();
                        }
                        catch(Exception ex) {
                        }
                        //Util.print("Removing entry " + entry);
                        val.removeElement(entry);
                        return;
                    }
                }
            }
        }
    }


    void removeEntry(String groupname, Address addr) {
        List val;
        AddressEntry entry;


        synchronized(groups) {
            val=(List)groups.get(groupname);
            if(val == null || val.size() == 0)
                return;
            for(Enumeration e2=val.elements(); e2.hasMoreElements();) {
                entry=(AddressEntry)e2.nextElement();
                if(entry.addr.equals(addr)) {
                    try {
                        if(entry.sock != null)
                            entry.sock.close();
                    }
                    catch(Exception ex) {
                    }
                    //Util.print("Removing entry " + entry);
                    val.removeElement(entry);
                    return;
                }
            }
        }
    }


    DataOutputStream findSocket(Address addr) {
        List val;
        AddressEntry entry;

        synchronized(groups) {
            for(Enumeration e=groups.keys(); e.hasMoreElements();) {
                val=(List)groups.get(e.nextElement());
                for(Enumeration e2=val.elements(); e2.hasMoreElements();) {
                    entry=(AddressEntry)e2.nextElement();
                    if(addr.equals(entry.addr))
                        return entry.output;
                }
            }
            return null;
        }
    }


    void sendToAllMembersInGroup(String groupname, byte[] msg) {
        List val;

        synchronized(groups) {
            val=(List)groups.get(groupname);
            if(val == null || val.size() == 0)
                return;
            for(Enumeration e=val.elements(); e.hasMoreElements();) {
                sendToMember(((AddressEntry)e.nextElement()).output, msg);
            }
        }
    }


    void sendToMember(DataOutputStream out, byte[] msg) {
        try {
            if(out != null) {
                out.writeInt(msg.length);
                out.write(msg, 0, msg.length);
            }
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Router_EXCEPTION_0, e);
            removeEntry(out); // closes socket
        }
    }


    static/*GemStoneAddition*/ class AddressEntry {
        Address addr=null;
        Socket sock=null;
        DataOutputStream output=null;


        public AddressEntry(Address addr, Socket sock, DataOutputStream output) {
            this.addr=addr;
            this.sock=sock;
            this.output=output;
        }


        void destroy() {
            if(output != null) {
                try {
                    output.close();
                }
                catch(Exception e) {
                }
                output=null;
            }
            if(sock != null) {
                try {
                    sock.close();
                }
                catch(Exception e) {
                }
                sock=null;
            }
        }

        @Override // GemStoneAddition
        public boolean equals(Object other) {
          if (other == null || !(other instanceof AddressEntry)) return false; // GemStoneAddition
            return addr.equals(((AddressEntry)other).addr);
        }

        @Override // GemStoneAddition
        public int hashCode() { // GemStoneAddition
          return addr.hashCode();
        }
        
        @Override // GemStoneAddition
        public String toString() {
            return "addr=" + addr + ", sock=" + sock;
        }
    }


    /** A SocketThread manages one connection to a client. Its main task is message routing. */
    class SocketThread extends Thread  {
        Socket sock=null;
        DataInputStream input=null;


        public SocketThread(Socket sock, DataInputStream ois) {
            this.sock=sock;
            input=ois;
        }

        void closeSocket() {
            try {
                if(input != null)
                    input.close();
                if(sock != null)
                    sock.close();
            }
            catch(Exception e) {
            }
        }


        @Override // GemStoneAddition
        public void run() {
            byte[] buf;
            int len;
            Address dst_addr=null;
            String gname;

            while(true) {
                try {
                    gname=input.readUTF(); // group name
                    len=input.readInt();
                    if(len == 0)
                        dst_addr=null;
                    else {
                        buf=new byte[len];
                        input.readFully(buf, 0, buf.length);  // dest address
                        dst_addr=(Address)Util.objectFromByteBuffer(buf);
                    }

                    len=input.readInt();
                    if(len == 0) {
                        if(log.isWarnEnabled()) log.warn("received null message");
                        continue;
                    }
                    buf=new byte[len];
                    input.readFully(buf, 0, buf.length);  // message
                    route(dst_addr, gname, buf);
                }
                catch(IOException io_ex) {

                        if(log.isInfoEnabled()) log.info("client " +
                                                                sock.getInetAddress().getHostName() + ':' + sock.getPort() +
                                                                " closed connection; removing it from routing table");
                    removeEntry(sock); // will close socket
                    return;
                }
                catch(Exception e) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.Router_EXCEPTION_0, e);
                    break;
                }
            }
            closeSocket();
        }

    }


    public static void main(String[] args) throws Exception {
        String arg;
        int port=8080;
        Router router=null;
        InetAddress address=null;
        System.out.println("Router is starting...");
        for(int i=0; i < args.length; i++) {
            arg=args[i];
            if("-help".equals(arg)) {
                System.out.println("Router [-port <port>] [-bindaddress <address>]");
                return;
            }
            else
                if("-port".equals(arg)) {
                    port=Integer.parseInt(args[++i]);
                }
                else
                    if("-bindaddress".equals(arg)) {
                        address=InetAddress.getByName(args[++i]);
                    }

        }



        try {
            if(address == null) router=new Router(port); else router=new Router(port, address);
            router.start();
            System.out.println("Router was created at " + new Date());
            System.out.println("Listening on port " + port + " and bound to address " + address);
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }


}
