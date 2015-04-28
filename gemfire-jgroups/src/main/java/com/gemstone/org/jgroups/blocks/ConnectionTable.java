/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ConnectionTable.java,v 1.39 2005/11/18 19:50:54 smarlownovell Exp $

package com.gemstone.org.jgroups.blocks;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Queue;
import com.gemstone.org.jgroups.util.QueueClosedException;
import com.gemstone.org.jgroups.util.Util;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;


/**
 * Manages incoming and outgoing TCP connections. For each outgoing message to destination P, if there
 * is not yet a connection for P, one will be created. Subsequent outgoing messages will use this
 * connection.  For incoming messages, one server socket is created at startup. For each new incoming
 * client connecting, a new thread from a thread pool is allocated and listens for incoming messages
 * until the socket is closed by the peer.<br>Sockets/threads with no activity will be killed
 * after some time.
 * <p>
 * Incoming messages from any of the sockets can be received by setting the message listener.
 * @author Bela Ban
 */
public class ConnectionTable implements Runnable {
    final HashMap       conns=new HashMap();       // keys: Addresses (peer address), values: Connection
    Receiver            receiver=null;
    ServerSocket        srv_sock=null;
//    boolean             reuse_addr=false; GemStoneAddition
    boolean             use_send_queues=true;
    InetAddress         bind_addr=null;

    /**
     * The address which will be broadcast to the group (the externally visible address which this host should
     * be contacted on). If external_addr is null, it will default to the same address that the server socket is bound to.
     */
    InetAddress		    external_addr=null;
    Address             local_addr=null;             // bind_addr + port of srv_sock
    int                 srv_port=7800;
    int                 max_port=0;                   // maximum port to bind to (if < srv_port, no limit)
    Thread              acceptor=null;               // continuously calls srv_sock.accept()
    static final int    backlog=20;                  // 20 conn requests are queued by ServerSocket (addtl will be discarded)
    int                 recv_buf_size=120000;
    int                 send_buf_size=60000;
    final Vector        conn_listeners=new Vector(); // listeners to be notified when a conn is established/torn down
    final Object        recv_mutex=new Object();     // to serialize simultaneous access to receive() from multiple Connections
    Reaper              reaper=null;                 // closes conns that have been idle for more than n secs
    long                reaper_interval=60000;       // reap unused conns once a minute
    long                conn_expire_time=300000;     // connections can be idle for 5 minutes before they are reaped
    boolean             use_reaper=false;            // by default we don't reap idle conns
    int                 sock_conn_timeout=1000;      // max time in millis to wait for Socket.connect() to return
    ThreadGroup         thread_group=null;
    protected final GemFireTracer log=GemFireTracer.getLog(getClass());
    final static        byte[] NULL_DATA={};
    final byte[]        cookie={'b', 'e', 'l', 'a'};



    /** Used for message reception. */
    public interface Receiver {
        void receive(Address sender, byte[] data, int offset, int length);
    }



    /** Used to be notified about connection establishment and teardown. */
    public interface ConnectionListener {
        void connectionOpened(Address peer_addr);
        void connectionClosed(Address peer_addr);
    }


    /**
     * Regular ConnectionTable without expiration of idle connections
     * @param srv_port The port on which the server will listen. If this port is reserved, the next
     *                 free port will be taken (incrementing srv_port).
     */
    public ConnectionTable(int srv_port) throws Exception {
        this.srv_port=srv_port;
        start();
    }


    public ConnectionTable(InetAddress bind_addr, int srv_port) throws Exception {
        this.srv_port=srv_port;
        this.bind_addr=bind_addr;
        start();
    }


    /**
     * ConnectionTable including a connection reaper. Connections that have been idle for more than conn_expire_time
     * milliseconds will be closed and removed from the connection table. On next access they will be re-created.
     * @param srv_port The port on which the server will listen
     * @param reaper_interval Number of milliseconds to wait for reaper between attepts to reap idle connections
     * @param conn_expire_time Number of milliseconds a connection can be idle (no traffic sent or received until
     *                         it will be reaped
     */
    public ConnectionTable(int srv_port, long reaper_interval, long conn_expire_time) throws Exception {
        this.srv_port=srv_port;
        this.reaper_interval=reaper_interval;
        this.conn_expire_time=conn_expire_time;
        use_reaper=true;
        start();
    }


    /**
     * Create a ConnectionTable
     * @param r A reference to a receiver of all messages received by this class. Method <code>receive()</code>
     *          will be called.
     * @param bind_addr The host name or IP address of the interface to which the server socket will bind.
     *                  This is interesting only in multi-homed systems. If bind_addr is null, the
     *	  	        server socket will bind to the first available interface (e.g. /dev/hme0 on
     *                  Solaris or /dev/eth0 on Linux systems).
     * @param external_addr The address which will be broadcast to the group (the externally visible address
     *                   which this host should be contacted on). If external_addr is null, it will default to
     *                   the same address that the server socket is bound to.
     * @param srv_port The port to which the server socket will bind to. If this port is reserved, the next
     *                 free port will be taken (incrementing srv_port).
     * @param max_port The largest port number that the server socket will be bound to. If max_port < srv_port
     *                 then there is no limit.
     */
    public ConnectionTable(Receiver r, InetAddress bind_addr, InetAddress external_addr, int srv_port, int max_port) throws Exception {
        setReceiver(r);
        this.bind_addr=bind_addr;
	    this.external_addr=external_addr;
        this.srv_port=srv_port;
	    this.max_port=max_port;
        start();
    }


    /**
     * ConnectionTable including a connection reaper. Connections that have been idle for more than conn_expire_time
     * milliseconds will be closed and removed from the connection table. On next access they will be re-created.
     *
     * @param r The Receiver 
     * @param bind_addr The host name or IP address of the interface to which the server socket will bind.
     *                  This is interesting only in multi-homed systems. If bind_addr is null, the
     *		        server socket will bind to the first available interface (e.g. /dev/hme0 on
     *		        Solaris or /dev/eth0 on Linux systems).
     * @param external_addr The address which will be broadcast to the group (the externally visible address
     *                   which this host should be contacted on). If external_addr is null, it will default to
     *                   the same address that the server socket is bound to.
     * @param srv_port The port to which the server socket will bind to. If this port is reserved, the next
     *                 free port will be taken (incrementing srv_port).
     * @param max_port The largest port number that the server socket will be bound to. If max_port < srv_port
     *                 then there is no limit.
     * @param reaper_interval Number of milliseconds to wait for reaper between attepts to reap idle connections
     * @param conn_expire_time Number of milliseconds a connection can be idle (no traffic sent or received until
     *                         it will be reaped
     */
    public ConnectionTable(Receiver r, InetAddress bind_addr, InetAddress external_addr, int srv_port, int max_port,
                           long reaper_interval, long conn_expire_time) throws Exception {
        setReceiver(r);
        this.bind_addr=bind_addr;
        this.external_addr=external_addr;
        this.srv_port=srv_port;
        this.max_port=max_port;
        this.reaper_interval=reaper_interval;
        this.conn_expire_time=conn_expire_time;
        use_reaper=true;
        start();
    }


    public void setReceiver(Receiver r) {
        receiver=r;
    }


    public void addConnectionListener(ConnectionListener l) {
        if(l != null && !conn_listeners.contains(l))
            conn_listeners.addElement(l);
    }


    public void removeConnectionListener(ConnectionListener l) {
        if(l != null) conn_listeners.removeElement(l);
    }


    public Address getLocalAddress() {
        if(local_addr == null)
            local_addr=bind_addr != null ? new IpAddress(bind_addr, srv_port) : null;
        return local_addr;
    }


    public int getSendBufferSize() {
        return send_buf_size;
    }

    public void setSendBufferSize(int send_buf_size) {
        this.send_buf_size=send_buf_size;
    }

    public int getReceiveBufferSize() {
        return recv_buf_size;
    }

    public void setReceiveBufferSize(int recv_buf_size) {
        this.recv_buf_size=recv_buf_size;
    }

    public int getSocketConnectionTimeout() {
        return sock_conn_timeout;
    }

    public void setSocketConnectionTimeout(int sock_conn_timeout) {
        this.sock_conn_timeout=sock_conn_timeout;
    }

    public int getNumConnections() {
      synchronized (conns) { // GemStoneAddition
        return conns.size();
      }
    }

    public boolean getUseSendQueues() {return use_send_queues;}
    public void setUseSendQueues(boolean flag) {this.use_send_queues=flag;}



    public void send(Address dest, byte[] data, int offset, int length) throws Exception {
        Connection conn;
        if(dest == null) {
            if(log.isErrorEnabled())
                log.error(ExternalStrings.ConnectionTable_DESTINATION_IS_NULL);
            return;
        }

        if(data == null) {
            log.warn("data is null; discarding packet");
            return;
        }

        // 1. Try to obtain correct Connection (or create one if not yet existent)
        try {
            conn=getConnection(dest);
            if(conn == null) return;
        }
        catch(Throwable ex) {
            throw new Exception("connection to " + dest + " could not be established", ex);
        }

        // 2. Send the message using that connection
        try {
            conn.send(data, offset, length);
        }
        catch(Throwable ex) {
            ex.printStackTrace();
            if(log.isTraceEnabled())
                log.trace("sending msg to " + dest + " failed (" + ex.getClass().getName() + "); removing from connection table");
            remove(dest);
        }
    }


    /** Try to obtain correct Connection (or create one if not yet existent) */
    Connection getConnection(Address dest) throws Exception {
        Connection conn=null;
        Socket sock;

        synchronized(conns) {
            conn=(Connection)conns.get(dest);
            if(conn == null) {
                // changed by bela Jan 18 2004: use the bind address for the client sockets as well
                SocketAddress tmpBindAddr=new InetSocketAddress(bind_addr, 0);
                InetAddress tmpDest=((IpAddress)dest).getIpAddress();
                SocketAddress destAddr=new InetSocketAddress(tmpDest, ((IpAddress)dest).getPort());
                sock=new Socket();
                sock.bind(tmpBindAddr);
                sock.connect(destAddr, sock_conn_timeout);

                try {
                    sock.setSendBufferSize(send_buf_size);
                }
                catch(IllegalArgumentException ex) {
                    if(log.isErrorEnabled()) log.error("exception setting send buffer size to " +
                            send_buf_size + " bytes", ex);
                }
                try {
                    sock.setReceiveBufferSize(recv_buf_size);
                }
                catch(IllegalArgumentException ex) {
                    if(log.isErrorEnabled()) log.error("exception setting receive buffer size to " +
                            send_buf_size + " bytes", ex);
                }
                conn=new Connection(sock, dest);
                conn.sendLocalAddress(local_addr);
                notifyConnectionOpened(dest);
                // conns.put(dest, conn);
                addConnection(dest, conn);
                conn.init();
                if(log.isInfoEnabled()) log.info(ExternalStrings.ConnectionTable_CREATED_SOCKET_TO__0, dest);
            }
            return conn;
        }
    }


    public void start() throws Exception {
        init();
        srv_sock=createServerSocket(srv_port, max_port);

        if (external_addr!=null)
            local_addr=new IpAddress(external_addr, srv_sock.getLocalPort());
        else if (bind_addr != null)
            local_addr=new IpAddress(bind_addr, srv_sock.getLocalPort());
        else
            local_addr=new IpAddress(srv_sock.getLocalPort());

        if(log.isInfoEnabled()) log.info(ExternalStrings.ConnectionTable_SERVER_SOCKET_CREATED_ON__0, local_addr);

        //Roland Kurmann 4/7/2003, build new thread group
        thread_group = new ThreadGroup(Thread.currentThread().getThreadGroup(), "ConnectionTableGroup");
        //Roland Kurmann 4/7/2003, put in thread_group
        acceptor=new Thread(thread_group, this, "ConnectionTable.AcceptorThread");
        acceptor.setDaemon(true);
        acceptor.start();

        // start the connection reaper - will periodically remove unused connections
        if(use_reaper && reaper == null) {
            reaper=new Reaper();
            reaper.start();
        }
    }

    protected void init() throws Exception {
    }
    
    /** Closes all open sockets, the server socket and all threads waiting for incoming messages */
    public void stop() {
        Iterator it=null;
        Connection conn;
        ServerSocket tmp;

        // 1. close the server socket (this also stops the acceptor thread)
        if(srv_sock != null) {
            try {
                tmp=srv_sock;
                srv_sock=null;
                tmp.close();
            }
            catch(Exception e) {
            }
        }


        // 2. then close the connections
        synchronized(conns) {
            it=conns.values().iterator();
            while(it.hasNext()) {
                conn=(Connection)it.next();
                conn.destroy();
            }
            conns.clear();
        }
        local_addr=null;
    }


    /**
     Remove <code>addr</code>from connection table. This is typically triggered when a member is suspected.
     */
    public void remove(Address addr) {
        Connection conn;

        synchronized(conns) {
            conn=(Connection)conns.remove(addr);
        }

        if(conn != null) {
            try {
                conn.destroy();  // won't do anything if already destroyed
            }
            catch(Exception e) {
            }
        }
        if(log.isTraceEnabled()) log.trace("removed " + addr + ", connections are " + toString());
    }


    /**
     * Acceptor thread. Continuously accept new connections. Create a new thread for each new
     * connection and put it in conns. When the thread should stop, it is
     * interrupted by the thread creator.
     */
    public void run() {
        Socket     client_sock;
        Connection conn=null;
        Address    peer_addr;

        while(srv_sock != null) {
            try {
                client_sock=srv_sock.accept();
                if(log.isTraceEnabled())
                    log.trace("accepted connection from " + client_sock.getInetAddress() + ":" + client_sock.getPort());
                try {
                    client_sock.setSendBufferSize(send_buf_size);
                }
                catch(IllegalArgumentException ex) {
                    if(log.isErrorEnabled()) log.error("exception setting send buffer size to " +
                           send_buf_size + " bytes", ex);
                }
                try {
                    client_sock.setReceiveBufferSize(recv_buf_size);
                }
                catch(IllegalArgumentException ex) {
                    if(log.isErrorEnabled()) log.error("exception setting receive buffer size to " +
                           send_buf_size + " bytes", ex);
                }

                // create new thread and add to conn table
                conn=new Connection(client_sock, null); // will call receive(msg)
                // get peer's address
                peer_addr=conn.readPeerAddress(client_sock);

                // client_addr=new IpAddress(client_sock.getInetAddress(), client_port);
                conn.setPeerAddress(peer_addr);

                synchronized(conns) {
                    if(conns.containsKey(peer_addr)) {
                        if(log.isTraceEnabled())
                            log.trace(peer_addr + " is already there, will reuse connection");
                        //conn.destroy();
                        //continue; // return; // we cannot terminate the thread (bela Sept 2 2004)
                    }
                    else {
                        // conns.put(peer_addr, conn);
                        addConnection(peer_addr, conn);
                        notifyConnectionOpened(peer_addr);
                    }
                }

                conn.init(); // starts handler thread on this socket
            }
            catch(SocketException sock_ex) {
                if(log.isInfoEnabled()) log.info(ExternalStrings.ConnectionTable_EXCEPTION_IS__0, sock_ex);
                if(conn != null)
                    conn.destroy();
                if(srv_sock == null)
                    break;  // socket was closed, therefore stop
            }
            catch(Throwable ex) {
                if(log.isWarnEnabled()) log.warn("exception is " + ex);
                if(srv_sock == null)
                    break;  // socket was closed, therefore stop
            }
        }
        if(log.isTraceEnabled())
            log.trace(Thread.currentThread().getName() + " terminated");
    }


    /**
     * Calls the receiver callback. We serialize access to this method because it may be called concurrently
     * by several Connection handler threads. Therefore the receiver doesn't need to synchronize.
     */
    public void receive(Address sender, byte[] data, int offset, int length) {
        if(receiver != null) {
            synchronized(recv_mutex) {
                receiver.receive(sender, data, offset, length);
            }
        }
        else
            if(log.isErrorEnabled()) log.error(ExternalStrings.ConnectionTable_RECEIVER_IS_NULL_NOT_SET_);
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer ret=new StringBuffer();
        Address key;
        Connection val;
        Map.Entry entry;
        HashMap copy;

        synchronized(conns) {
            copy=new HashMap(conns);
        }
        ret.append("connections (" + copy.size() + "):\n");
        for(Iterator it=copy.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(Address)entry.getKey();
            val=(Connection)entry.getValue();
            ret.append("key: " + key + ": " + val + '\n');
        }
        ret.append('\n');
        return ret.toString();
    }


    /** Finds first available port starting at start_port and returns server socket.
      * Will not bind to port >end_port. Sets srv_port */
    protected ServerSocket createServerSocket(int start_port, int end_port) throws Exception {
        ServerSocket ret=null;

        while(true) {
            try {
                if(bind_addr == null)
                    ret=new ServerSocket(start_port);
                else {

                    ret=new ServerSocket(start_port, backlog, bind_addr);
                }
            }
            catch(SocketException bind_ex) {
              // GemStoneAddition
              if (Util.treatAsBindException(bind_ex)) {
                if (start_port==end_port) throw new BindException("No available port to bind to");
                  if(bind_addr != null) {
                    NetworkInterface nic=NetworkInterface.getByInetAddress(bind_addr);
                    if(nic == null)
                        throw new BindException("bind_addr " + bind_addr + " is not a valid interface");
                }
                start_port++;
                continue;
              } else {
                //not a manifestation of BindException, handle like IOException
                if(log.isErrorEnabled()) log.error(ExternalStrings.ConnectionTable_EXCEPTION_IS__0, bind_ex);
              }
            }
            catch(IOException io_ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ConnectionTable_EXCEPTION_IS__0, io_ex);
            }
            srv_port=start_port;
            break;
        }
        return ret;
    }


    void notifyConnectionOpened(Address peer) {
        if(peer == null) return;
        for(int i=0; i < conn_listeners.size(); i++)
            ((ConnectionListener)conn_listeners.elementAt(i)).connectionOpened(peer);
    }

    void notifyConnectionClosed(Address peer) {
        if(peer == null) return;
        for(int i=0; i < conn_listeners.size(); i++)
            ((ConnectionListener)conn_listeners.elementAt(i)).connectionClosed(peer);
    }


    void addConnection(Address peer, Connection c) {
      synchronized (conns) { // GemStoneAddition
        conns.put(peer, c);
      }
        if(reaper != null && !reaper.isRunning())
            reaper.start();
    }




    class Connection implements Runnable {
        Socket           sock=null;                // socket to/from peer (result of srv_sock.accept() or new Socket())
        String           sock_addr=null;           // used for Thread.getName()
        DataOutputStream out=null;                 // for sending messages
        DataInputStream  in=null;                  // for receiving messages
        Thread           receiverThread=null;      // thread for receiving messages // GemStoneAddition - accesses synchronized via this
        Address          peer_addr=null;           // address of the 'other end' of the connection
        final Object     send_mutex=new Object();  // serialize sends
        long             last_access=System.currentTimeMillis(); // last time a message was sent or received

        /** Queue of byte[] of data to be sent to the peer of this connection */
        Queue            send_queue=new Queue();
        Sender           sender=new Sender();


        protected String getSockAddress() {
            if(sock_addr != null)
                return sock_addr;
            if(sock != null) {
                StringBuffer sb=new StringBuffer();
                sb.append(sock.getLocalAddress().getHostAddress()).append(':').append(sock.getLocalPort());
                sb.append(" - ").append(sock.getInetAddress().getHostAddress()).append(':').append(sock.getPort());
                sock_addr=sb.toString();
            }
            return sock_addr;
        }




        Connection(Socket s, Address peer_addr) {
            sock=s;
            this.peer_addr=peer_addr;
            try {
                // out=new DataOutputStream(sock.getOutputStream());
                // in=new DataInputStream(sock.getInputStream());

                // The change to buffered input and output stream yielded a 400% performance gain !
                // bela Sept 7 2006
                out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
                in=new DataInputStream(new BufferedInputStream(sock.getInputStream()));

                // in=new DataInputStream(sock.getInputStream());
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ConnectionTable_EXCEPTION_IS__0, ex);
            }
        }


        synchronized /* GemStoneAddition */ boolean established() {
            return receiverThread != null && receiverThread.isAlive() /* GemStoneAddition */;
        }


        void setPeerAddress(Address peer_addr) {
            this.peer_addr=peer_addr;
        }

        Address getPeerAddress() {return peer_addr;}

        void updateLastAccessed() {
            last_access=System.currentTimeMillis();
        }

        synchronized /* GemStoneAddition */ void init() {
            // if(log.isInfoEnabled()) log.info("connection was created to " + peer_addr);
            if(receiverThread == null || !receiverThread.isAlive()) {
                // Roland Kurmann 4/7/2003, put in thread_group
                receiverThread=new Thread(thread_group, this, "ConnectionTable.Connection.Receiver [" + getSockAddress() + "]");
                receiverThread.setDaemon(true);
                receiverThread.start();
                if(log.isTraceEnabled())
                    log.trace("ConnectionTable.Connection.Receiver started");
            }
        }


        synchronized /* GemStoneAddition */ void destroy() {
            closeSocket(); // should terminate handler as well
            sender.stop();
            if (receiverThread != null) receiverThread.interrupt(); // GemStoneAddition
            receiverThread=null;
        }


        void send(byte[] data, int offset, int length) {
            if(use_send_queues) {
                try {
                    if(data != null) {
                        // we need to copy the byte[] buffer here because the original buffer might get changed
                        // in the meantime
                        byte[] tmp=new byte[length];
                        System.arraycopy(data, offset, tmp, 0, length);
                        send_queue.add(tmp);}
                    else {
                        send_queue.add(NULL_DATA);
                    }
                    if(!sender.isRunning())
                        sender.start();
                }
                catch(QueueClosedException e) {
                    log.error(ExternalStrings.ConnectionTable_FAILED_ADDING_MESSAGE_TO_SEND_QUEUE, e);
                }
            }
            else
                _send(data, offset, length);
        }


        protected/*GemStoneAddition*/ void _send(byte[] data, int offset, int length) {
            synchronized(send_mutex) {
                try {
                    doSend(data, offset, length);
                    updateLastAccessed();
                }
                catch(IOException io_ex) {
                    if(log.isWarnEnabled())
                        log.warn("peer closed connection, trying to re-establish connection and re-send msg");
                    try {
                        doSend(data, offset, length);
                        updateLastAccessed();
                    }
                    catch(IOException io_ex2) {
                         if(log.isErrorEnabled()) log.error(ExternalStrings.ConnectionTable_2ND_ATTEMPT_TO_SEND_DATA_FAILED_TOO);
                    }
                    catch(Exception ex2) {
                         if(log.isErrorEnabled()) log.error(ExternalStrings.ConnectionTable_EXCEPTION_IS__0, ex2);
                    }
                }
                catch(Throwable ex) {
                     if(log.isErrorEnabled()) log.error(ExternalStrings.ConnectionTable_EXCEPTION_IS__0, ex);
                }
            }
        }


        void doSend(byte[] data, int offset, int length) throws Exception {
            try {
                // we're using 'double-writes', sending the buffer to the destination in 2 pieces. this would
                // ensure that, if the peer closed the connection while we were idle, we would get an exception.
                // this won't happen if we use a single write (see Stevens, ch. 5.13).
                if(out != null) {
                    out.writeInt(length); // write the length of the data buffer first
                    Util.doubleWrite(data, offset, length, out);
                    out.flush();  // may not be very efficient (but safe)
                }
            }
            catch(Exception ex) {
                if(log.isErrorEnabled())
                    log.error(ExternalStrings.ConnectionTable_FAILURE_SENDING_TO__0, peer_addr, ex);
                remove(peer_addr);
                throw ex;
            }
        }


        /**
         * Reads the peer's address. First a cookie has to be sent which has to match my own cookie, otherwise
         * the connection will be refused
         */
        Address readPeerAddress(Socket client_sock) throws Exception {
            Address     client_peer_addr=null;
            byte[]      input_cookie=new byte[cookie.length];
            int         client_port=client_sock != null? client_sock.getPort() : 0;
            short       version;
            InetAddress client_addr=client_sock != null? client_sock.getInetAddress() : null;

            if(in != null) {
                initCookie(input_cookie);

                // read the cookie first
                if (input_cookie.length != in.read(input_cookie, 0, input_cookie.length)) {
                  throw new SocketException("Failed to read input cookie"); // GemStoneAddition
                }
                if(!matchCookie(input_cookie))
                    throw new SocketException("ConnectionTable.Connection.readPeerAddress(): cookie sent by " +
                                              client_peer_addr + " does not match own cookie; terminating connection");
                // then read the version
                version=in.readShort();

                if(JGroupsVersion.compareTo(version) == false) {
                    if(log.isWarnEnabled())
                        log.warn(new StringBuffer("packet from ").append(client_addr).append(':').append(client_port).
                               append(" has different version (").append(version).append(") from ours (").
                                 append(JGroupsVersion.version).append("). This may cause problems"));
                }
                client_peer_addr=new IpAddress();
                client_peer_addr.readFrom(in);

                updateLastAccessed();
            }
            return client_peer_addr;
        }


        /**
         * Send the cookie first, then the our port number. If the cookie doesn't match the receiver's cookie,
         * the receiver will reject the connection and close it.
         */
        void sendLocalAddress(Address local_addr) {
            if(local_addr == null) {
                if(log.isWarnEnabled()) log.warn("local_addr is null");
                return;
            }
            if(out != null) {
                try {
                    // write the cookie
                    out.write(cookie, 0, cookie.length);

                    // write the version
                    out.writeShort(JGroupsVersion.version);
                    local_addr.writeTo(out);
                    out.flush(); // needed ?
                    updateLastAccessed();
                }
                catch(Throwable t) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.ConnectionTable_EXCEPTION_IS__0, t);
                }
            }
        }


        void initCookie(byte[] c) {
            if(c != null)
                for(int i=0; i < c.length; i++)
                    c[i]=0;
        }

        boolean matchCookie(byte[] input) {
            if(input == null || input.length < cookie.length) return false;
            if(log.isInfoEnabled()) log.info(ExternalStrings.ConnectionTable_INPUT_COOKIE_IS__0, printCookie(input));
            for(int i=0; i < cookie.length; i++)
                if(cookie[i] != input[i]) return false;
            return true;
        }


        String printCookie(byte[] c) {
            if(c == null) return "";
            return new String(c);
        }


        public void run() {
            byte[] buf=new byte[256]; // start with 256, increase as we go
            int len=0;

            for (;;) { // GemStoneAddition -- remove coding anti-pattern
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                try {
                    if(in == null) {
                        if(log.isErrorEnabled()) log.error(ExternalStrings.ConnectionTable_INPUT_STREAM_IS_NULL_);
                        break;
                    }
                    len=in.readInt();
                    if(len > buf.length)
                        buf=new byte[len];
                    in.readFully(buf, 0, len);
                    updateLastAccessed();
                    receive(peer_addr, buf, 0, len); // calls receiver.receive(msg)
                }
                catch(OutOfMemoryError mem_ex) {
                    if(log.isWarnEnabled()) log.warn("dropped invalid message, closing connection");
                    break; // continue;
                }
                catch(EOFException eof_ex) {  // peer closed connection
                    if(log.isInfoEnabled()) log.info(ExternalStrings.ConnectionTable_EXCEPTION_IS__0, eof_ex);
                    notifyConnectionClosed(peer_addr);
                    break;
                }
                catch(IOException io_ex) {
                    if(log.isInfoEnabled()) log.info(ExternalStrings.ConnectionTable_EXCEPTION_IS__0, io_ex);
                    notifyConnectionClosed(peer_addr);
                    break;
                }
                catch(Throwable e) {
                    if(log.isWarnEnabled()) log.warn("exception is " + e);
                }
            }
            if(log.isTraceEnabled())
                log.trace("ConnectionTable.Connection.Receiver terminated");
//            receiverThread=null; GemStoneAddition
            closeSocket();
            remove(peer_addr);
        }


        @Override // GemStoneAddition
        public String toString() {
            StringBuffer ret=new StringBuffer();
            InetAddress local=null, remote=null;
            String local_str, remote_str;

            if(sock == null)
                ret.append("<null socket>");
            else {
                //since the sock variable gets set to null we want to make
                //make sure we make it through here without a nullpointer exception
                Socket tmp_sock=sock;
                local=tmp_sock.getLocalAddress();
                remote=tmp_sock.getInetAddress();
                local_str=local != null ? Util.shortName(local) : "<null>";
                remote_str=remote != null ? Util.shortName(remote) : "<null>";
                ret.append('<' + local_str + ':' + tmp_sock.getLocalPort() +
                           " --> " + remote_str + ':' + tmp_sock.getPort() + "> (" +
                           ((System.currentTimeMillis() - last_access) / 1000) + " secs old)");
                tmp_sock=null;
            }

            return ret.toString();
        }


        void closeSocket() {
            if(sock != null) {
                try {
                    sock.close(); // should actually close in/out (so we don't need to close them explicitly)
                }
                catch(Exception e) {
                }
                sock=null;
            }
            if(out != null) {
                try {
                    out.close(); // flushes data
                }
                catch(Exception e) {
                }
                // removed 4/22/2003 (request by Roland Kurmann)
                // out=null;
            }
            if(in != null) {
                try {
                    in.close();
                }
                catch(Exception ex) {
                }
                in=null;
            }
        }


        class Sender implements Runnable {
            Thread senderThread; // GemStoneAddition - synchronized on this
//            private boolean running=false; GemStoneAddition remove coding anti-pattern

            synchronized /* GemStoneAddition */ void start() {
                if(senderThread == null || !senderThread.isAlive()) {
                    senderThread=new Thread(thread_group, this, "ConnectionTable.Connection.Sender [" + getSockAddress() + "]");
                    senderThread.setDaemon(true);
                    senderThread.start();
//                    running=true; GemStoneAddition
                    if(log.isTraceEnabled())
                        log.trace("ConnectionTable.Connection.Sender thread started");
                }
            }

            synchronized /* GemStoneAddition */ void stop() {
                if(senderThread != null) {
                    senderThread.interrupt();
                    senderThread=null;
//                    running=false; GemStoneAddition
                }
            }

            synchronized /* GemStoneAddition */ boolean isRunning() {
                return /* running && */ senderThread != null && senderThread.isAlive() /* GemStoneAddition */;
            }

            public void run() {
                byte[] data;
                for (;;) { // GemStoneAddition remove coding anti-pattern
                  if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                    try {
                        data=(byte[])send_queue.remove();
                        if(data == null)
                            continue;
                        _send(data, 0, data.length);
                    }
                    catch (InterruptedException ie) { // GemStoneAddition
                        // no need to reset the bit; we're exiting
                        break; // exit loop and thread
                    }
                    catch(QueueClosedException e) {
                        break;
                    }
                }
//                running=false; GemStoneAddition
                if(log.isTraceEnabled())
                    log.trace("ConnectionTable.Connection.Sender thread terminated");
            }
        }


    }


    class Reaper implements Runnable {
        Thread t=null; // GemStoneAddition synchronize on this to access

        Reaper() {
            ;
        }

        public void start() {
            if(conns.size() == 0)
                return;
            synchronized (this) { // GemStoneAddition
//            if(t != null && !t.isAlive())
//                t=null;       GemStoneAddition
            if(t == null || !t.isAlive()) {
                //RKU 7.4.2003, put in threadgroup
                t=new Thread(thread_group, this, "ConnectionTable.ReaperThread");
                t.setDaemon(true); // will allow us to terminate if all remaining threads are daemons
                t.start();
            }
            }
        }

        public void stop() {
          synchronized (this) { // GemStoneAddition
            if(t != null) {
                t.interrupt(); // GemStoneAddition
                t=null;
            }
          }
        }


        synchronized /* GemStoneAddition */ public boolean isRunning() {
            return t != null && t.isAlive() /* GemStoneAddition */;
        }

        public void run() {
            Connection value;
            Map.Entry entry;
            long curr_time;

            if(log.isInfoEnabled()) log.info("connection reaper thread was started. Number of connections=" +
                                             conns.size() + ", reaper_interval=" + reaper_interval + ", conn_expire_time=" +
                                             conn_expire_time);

            for (;;) { // GemStoneAddition remove coding anti-pattern 
                // first sleep
//              if (conns.size() == 0) break; // GemStoneAddition but needs to be synchronized
              try { // GemStoneAddition
                Util.sleep(reaper_interval);
              }
              catch (InterruptedException e) {
                // Thread.currentThread().interrupt(); not needed; we're exiting
                break; // exit loop and thread
              }
                synchronized(conns) {
                    if (conns.size() == 0) break; // GemStoneAddition
                    curr_time=System.currentTimeMillis();
                    for(Iterator it=conns.entrySet().iterator(); it.hasNext();) {
                        entry=(Map.Entry)it.next();
                        value=(Connection)entry.getValue();
                        if(log.isInfoEnabled()) log.info("connection is " +
                                                         ((curr_time - value.last_access) / 1000) + " seconds old (curr-time=" +
                                                         curr_time + ", last_access=" + value.last_access + ')');
                        if(value.last_access + conn_expire_time < curr_time) {
                            if(log.isInfoEnabled()) log.info("connection " + value +
                                                             " has been idle for too long (conn_expire_time=" + conn_expire_time +
                                                             "), will be removed");
                            value.destroy();
                            it.remove();
                        }
                    }
                }
            }
            if(log.isInfoEnabled()) log.info(ExternalStrings.ConnectionTable_REAPER_TERMINATED);
//            t=null; GemStoneAddition
        }
    }


}

