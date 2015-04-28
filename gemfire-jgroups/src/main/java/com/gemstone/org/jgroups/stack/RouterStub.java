/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: RouterStub.java,v 1.15 2005/12/08 09:34:47 belaban Exp $

package com.gemstone.org.jgroups.stack;


import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.protocols.TunnelHeader;
import com.gemstone.org.jgroups.util.Buffer;
import com.gemstone.org.jgroups.util.ExposedByteArrayOutputStream;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.List;
import com.gemstone.org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.net.Socket;




public class RouterStub  {
    String router_host=null;       // name of the router host
    int router_port=0;          // port on which router listens on router_host
    Socket sock=null;              // socket connecting to the router
    final ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(512);
    DataOutputStream output=null;            // output stream associated with sock
    DataInputStream input=null;             // input stream associated with sock
    Address local_addr=null;        // addr of group mbr. Once assigned, remains the same
    static final long RECONNECT_TIMEOUT=5000; // msecs to wait until next connection retry attempt
    private volatile boolean connected=false;

    private volatile boolean reconnect=false;   // controls reconnect() loop

    protected static final GemFireTracer log=GemFireTracer.getLog(RouterStub.class);


    /**
     Creates a stub for a remote Router object.
     @param router_host The name of the router's host
     @param router_port The router's port
     */
    public RouterStub(String router_host, int router_port) {
        this.router_host=router_host != null? router_host : "localhost";
        this.router_port=router_port;
    }


    public boolean isConnected() {
        return connected;
    }

    /**
     * Establishes a connection to the router. The router will send my address (its peer address) back
     * as an Address, which is subsequently returned to the caller. The reason for not using
     * InetAddress.getLocalHost() or sock.getLocalAddress() is that this may not be permitted
     * with certain security managers (e.g if this code runs in an applet). Also, some network
     * address translation (NAT) (e.g IP Masquerading) may return the wrong address.
     */
    public synchronized Address connect() throws Exception {
        Address ret=null;
        int len=0;
        byte[] buf;

        try {
            sock=new Socket(router_host, router_port);
            sock.setSoLinger(true, 500);

            // Retrieve our own address by reading it from the socket
            input=new DataInputStream(sock.getInputStream());
            len=input.readInt();
            buf=new byte[len];
            input.readFully(buf);
            ret=(Address)Util.objectFromByteBuffer(buf);
            output=new DataOutputStream(sock.getOutputStream());
            connected=true;
        }
        catch(Exception e) {
            connected=false;
            if(sock != null)
                sock.close();
            throw e;
        }

        // IpAddress uses InetAddress.getLocalHost() to find the host. May not be permitted in applets !
        if(ret == null && sock != null)
            ret=new com.gemstone.org.jgroups.stack.IpAddress(sock.getLocalPort());

        // set local address; this is the one that we will use from now on !
        if(local_addr == null)
            local_addr=ret;

        return ret;
    }


    /** Closes the socket and the input and output streams associated with it */
    public synchronized void disconnect() {
        if(output != null) {
            try {
                output.close();
                output=null;
            }
            catch(Exception e) {
            }
        }

        if(input != null) {
            try {
                input.close();
                input=null;
            }
            catch(Exception e) {
            }
        }

        if(sock != null) {
            try {
                sock.close();
                sock=null;
            }
            catch(Exception e) {
            }
        }
        connected=false;
        // stop the TUNNEL receiver thread
        reconnect=false;
    }


    /**
     Register this process with the router under <code>groupname</code>.
     @param groupname The name of the group under which to register
     @return boolean False if connection down, true if registration successful.
     */
    public boolean register(String groupname) {
        byte[] buf=null;

        if(sock == null || output == null || input == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RouterStub_NO_CONNECTION_TO_ROUTER_GROUPNAME_0, groupname);
            connected=false;
            return false;
        }

        if(groupname == null || groupname.length() == 0) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RouterStub_GROUPNAME_IS_NULL);
            return false;
        }

        if(local_addr == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RouterStub_LOCAL_ADDR_IS_NULL);
            return false;
        }

        try {
            buf=Util.objectToByteBuffer(local_addr);
            output.writeInt(Router.REGISTER);
            output.writeUTF(groupname);
            output.writeInt(buf.length);
            output.write(buf, 0, buf.length);  // local_addr
            output.flush();
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RouterStub_FAILURE__0, Util.getStackTrace(e));
            connected=false;
            return false;
        }
        return true;
    }


    /**
     Retrieves the membership (list of Addresses) for a given group. This is mainly used by the PING
     protocol to obtain its initial membership. This is used infrequently, so don't maintain socket
     for the entire time, but create/delete it on demand.
     */
    public List get(String groupname) {
        List ret=null;
        Socket tmpsock=null;
        DataOutputStream tmpOutput=null;
        DataInputStream tmpInput=null;
        int len;
        byte[] buf;


        if(groupname == null || groupname.length() == 0) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RouterStub_GROUPNAME_IS_NULL);
            return null;
        }

        try {
            tmpsock=new Socket(router_host, router_port);
            tmpsock.setSoLinger(true, 500);
            tmpInput=new DataInputStream(tmpsock.getInputStream());

            len=tmpInput.readInt();     // discard my own address
            buf=new byte[len];       // (first thing returned by router on acept())
            tmpInput.readFully(buf);
            tmpOutput=new DataOutputStream(tmpsock.getOutputStream());

            // request membership for groupname
            tmpOutput.writeInt(Router.GET);
            tmpOutput.writeUTF(groupname);

            // wait for response (List)
            len=tmpInput.readInt();
            if(len == 0)
                return null;

            buf=new byte[len];
            tmpInput.readFully(buf);
            ret=(List)Util.objectFromByteBuffer(buf);
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RouterStub_EXCEPTION_0, e);
        }
        finally {
            try {
                if(tmpOutput != null) tmpOutput.close();
            }
            catch(Exception e) {
            }
            try {
                if(tmpInput != null) tmpInput.close();
            }
            catch(Exception e) {
            }
            try {
                if(tmpsock != null) tmpsock.close();
            }
            catch(Exception e) {
            }
        }
        return ret;
    }


    /** Sends a message to the router. Returns false if message cannot be sent (e.g. no connection to
     router, true otherwise. */
    public boolean send(Message msg, String groupname) {
        Address dst_addr=null;

        if(sock == null || output == null || input == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RouterStub_NO_CONNECTION_TO_ROUTER_GROUPNAME_0, groupname);
            connected=false;
            return false;
        }

        if(msg == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RouterStub_MESSAGE_IS_NULL);
            return false;
        }

        try {
            dst_addr=msg.getDest(); // could be null in case of mcast
            out_stream.reset();
            DataOutputStream tmp=new DataOutputStream(out_stream);
            msg.writeTo(tmp);
            tmp.close();
            Buffer buf=new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());

            // 1. Group name
            output.writeUTF(groupname);

            // 2. Destination address
            Util.writeAddress(dst_addr, output);

            // 3. Length of byte buffer
            output.writeInt(buf.getLength());

            // 4. Byte buffer
            output.write(buf.getBuf(), 0, buf.getLength());
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RouterStub_FAILED_SENDING_MESSAGE_TO__0, dst_addr, e);
            connected=false;
            return false;
        }
        return true;
    }


    /** Receives a message from the router (blocking mode). If the connection is down,
     false is returned, otherwise true */
    public Message receive() {
        Message ret=null;
        byte[] buf=null;
        int len;

        if(sock == null || output == null || input == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.RouterStub_NO_CONNECTION_TO_ROUTER);
            connected=false;
            return null;
        }
        Address dest;
        try {
            dest=Util.readAddress(input);
            len=input.readInt();
            if(len == 0) {
//                ret=null; GemStoneAddition  (redundant assignment)
            }
            else {
                buf=new byte[len];
                input.readFully(buf, 0, len);
                ret=new Message(false);
                ByteArrayInputStream tmp=new ByteArrayInputStream(buf);
                DataInputStream in=new DataInputStream(tmp);
                ret.readFrom(in);
                ret.setDest(dest);
                in.close();
            }
        }
        catch(Exception e) {
            if (connected) {
                if(log.isTraceEnabled()) log.trace("failed receiving message", e);
            }
            connected=false;
            return null;
        }

        if(log.isTraceEnabled()) log.trace("received "+ret);
        return ret;
    }


    /** Tries to establish connection to router. Tries until router is up again. */
    public boolean reconnect(int max_attempts) {
        Address new_addr=null;
        int num_atttempts=0;

        if(connected) return false;
        disconnect();
        reconnect=true;
        while(reconnect && (num_atttempts++ < max_attempts || max_attempts == -1)) {
            try {
                if((new_addr=connect()) != null)
                    break;
            }
            catch(Exception ex) { // this is a normal case
                if(log.isTraceEnabled()) log.trace("failed reconnecting", ex);
            }
            if(max_attempts == -1) {
              try { // GemStoneAddition
                Util.sleep(RECONNECT_TIMEOUT);
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false; // treat like we ran out of attempts
              }
            }
        }
        if(new_addr == null) {
            return false;
        }
        if(log.isTraceEnabled()) log.trace("client reconnected, new address is " + new_addr);
        return true;
    }


     public boolean reconnect() {
         return reconnect(-1);
     }

//    public static void main(String[] args) {
//        if(args.length != 2) {
//            System.out.println("RouterStub <host> <port>");
//            return;
//        }
//        RouterStub stub=new RouterStub(args[0], Integer.parseInt(args[1]));
//        Address my_addr;
//        boolean rc;
//        final String groupname="BelaGroup";
//        Message msg;
//        List mbrs;
//
//        try {
//            my_addr=stub.connect();
//            System.out.println("My address is " + my_addr);
//
//            System.out.println("Registering under " + groupname);
//            rc=stub.register(groupname);
//            System.out.println("Done, rc=" + rc);
//
//
//            System.out.println("Getting members of " + groupname + ": ");
//            mbrs=stub.get(groupname);
//            System.out.println("Done, mbrs are " + mbrs);
//
//
//            for(int i=0; i < 10; i++) {
//                msg=new Message(null, my_addr, "Bela #" + i);
//                msg.putHeader("TUNNEL", new TunnelHeader(groupname));
//                rc=stub.send(msg, groupname);
//                System.out.println("Sent msg, rc=" + rc);
//            }
//
//            for(int i=0; i < 10; i++) {
//                System.out.println("stub.receive():");
//                msg=stub.receive();
//                System.out.println("Received msg");
//            }
//
//        }
//        catch(Exception ex) {
//            log.error(ex);
//        }
//        finally {
//            stub.disconnect();
//        }
//    }


}
