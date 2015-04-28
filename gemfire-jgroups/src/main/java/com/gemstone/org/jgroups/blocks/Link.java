/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Link.java,v 1.7 2005/05/30 16:14:33 belaban Exp $

package com.gemstone.org.jgroups.blocks;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.TimedWriter;
import com.gemstone.org.jgroups.util.Util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;





/**
 * Implements a physical link between 2 parties (point-to-point connection). For incoming traffic,
 * a server socket is created (bound to a given local address and port). The receiver thread does the
 * following: it accepts a new connection from the server socket and (on the same thread) reads messages
 * until the connection breaks. Then it goes back to accept(). This is done in 2 nested while-loops.
 * The outgoing connection is established when started. If this fails, the link is marked as not established.
 * This means that there is not outgoing socket.<br>
 * A heartbeat will be exchanged between the 2 peers periodically as long as the connection is established
 * (outgoing socket is okay). When the connection breaks, the heartbeat will stop and a connection establisher
 * thread will be started. It periodically tries to re-establish connection to the peer. When this happens
 * it will stop and the heartbeat thread will resume.<br>
 * For details see Link.txt
 * @author  Bela Ban, June 2000
 */
@SuppressFBWarnings(value="DE_MIGHT_IGNORE",justification="GemFire does not use this class")
public class Link implements Runnable {
    String                 local_addr=null, remote_addr=null;
    InetAddress            local=null, remote=null;
    int                    local_port=0, remote_port=0;
    ServerSocket           srv_sock=null;
    Socket                 outgoing=null;          // traffic to peer
    Socket                 incoming=null;          // traffic from peer
    DataOutputStream       outstream=null;
    DataInputStream        instream=null;
    boolean                established=false;  // (incoming and outgoing) connections to peer are up and running
//    boolean                stop=false; GemStoneAddition
    boolean                trace=false;
    
    // GemStoneAddition access receiver_thread while synchronized on this
    Thread                 receiver_thread=null;
    static/*GemStoneAddition*/ final long             receiver_thread_join_timeout=2000;
    Receiver               receiver=null;
    static final int       HB_PACKET=-99;
    Heartbeat              hb=null;
    long                   timeout=10000;  // if no heartbeat was received for timeout msecs, assume peer is dead
    long                   hb_interval=3000;        // send a heartbeat every n msecs
    final Object           outgoing_mutex=new Object();  // sync on creation and closing of outgoing socket
    TimedWriter            writer=null;
    GemFireTracer                    log=GemFireTracer.getLog(getClass());


    public interface Receiver {
	void receive(byte[] msg);
	void linkDown(InetAddress local, int local_port, InetAddress remote, int remote_port);
	void linkUp(InetAddress local, int local_port, InetAddress remote, int remote_port);
	void missedHeartbeat(InetAddress local, int local_port, InetAddress remote, int remote_port, int num_hbs);
	void receivedHeartbeatAgain(InetAddress local, int local_port, InetAddress remote, int remote_port);
    }


    
    public Link(String local_addr, int local_port, String remote_addr, int remote_port) {
	this.local_addr=local_addr;   this.local_port=local_port;
	this.remote_addr=remote_addr; this.remote_port=remote_port;
	hb=new Heartbeat(timeout, hb_interval);
    }


    public Link(String local_addr, int local_port, String remote_addr, int remote_port, Receiver r) {
	this(local_addr, local_port, remote_addr, remote_port);
	setReceiver(r);
    }



    public Link(String local_addr, int local_port, String remote_addr, int remote_port, 
		long timeout, long hb_interval, Receiver r) {
	this.local_addr=local_addr;   this.local_port=local_port;
	this.remote_addr=remote_addr; this.remote_port=remote_port;
	this.timeout=timeout; this.hb_interval=hb_interval;
	hb=new Heartbeat(timeout, hb_interval);
	setReceiver(r);
    }


    public void          setTrace(boolean t)     {trace=t;}
    public void          setReceiver(Receiver r) {receiver=r;}
    public boolean       established()           {return established;}
    public InetAddress   getLocalAddress()       {return local;}
    public InetAddress   getRemoteAddress()      {return remote;}
    public int           getLocalPort()          {return local_port;}
    public int           getRemotePort()         {return remote_port;}





    public void start() throws Exception {
	local=InetAddress.getByName(local_addr);
	remote=InetAddress.getByName(remote_addr);
	srv_sock=new ServerSocket(local_port, 1, local);
	createOutgoingConnection(hb_interval);   // connection to peer established, sets established=true
	startReceiverThread();                   // start reading from incoming socket
	hb.start();                              // starts heartbeat (conn establisher is not yet started)
    }



    public void stop() {
	stopReceiverThread();
	hb.stop();
	try {srv_sock.close();}	catch(Exception e) {}
	established=false;
    }





    /** Tries to send buffer across out socket. Tries to establish connection if not yet connected. */
    public boolean send(byte[] buf) {
	if(buf == null || buf.length == 0) {
	    if(trace) System.err.println("Link.send(): buffer is null or does not contain any data !");
	    return false;
	}
	if(!established) { // will be set by ConnectionEstablisher when connection has been set up
	    if(trace) log.error(ExternalStrings.Link_LINKSEND_CONNECTION_NOT_ESTABLISHED_DISCARDING_MESSAGE);
	    return false;
	}

	try {
	    outstream.writeInt(buf.length);  // synchronized anyway
	    outstream.write(buf);            // synchronized anyway, we don't need to sync on outstream
	    return true;
	}
	catch(Exception ex) {  // either IOException or EOFException (subclass if IOException)
	    if(trace) log.error(ExternalStrings.Link_LINKSEND1_SENDING_FAILED_RETRYING);
	    return retry(buf);
	}
    }


    boolean retry(byte[] buf) {
	closeOutgoingConnection();	        // there something wrong, close connection
	if(!createOutgoingConnection()) {       // ... and re-open. if this fails,
	    closeOutgoingConnection();          // just abort and return failure to caller
	    return false;
	}
	else {
	    try {
		outstream.writeInt(buf.length);
		outstream.write(buf);
		return true;
	    }
	    catch(Exception e) {
		if(trace) System.out.println("Link.send2(): failed, closing connection");
		closeOutgoingConnection();
		return false;
	    }
	}
    }




    /** Receiver thread main loop. Accept a connection and then read on it until the connection
	breaks. Only then is the next connection handled. The reason is that there is only supposed
	to be 1 connection to this server socket at the same time. 
    */
    public void run() {
	int              num_bytes;
	byte[]           buf;
	InetAddress      peer=null;
	int              peer_port=0;

	for (;;)  { // GemStoneAddition remove coding anti-pattern while(!stop)
          if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
	    try {
		if(trace) System.out.println("-- WAITING for ACCEPT");
		incoming=srv_sock.accept();
		instream=new DataInputStream(incoming.getInputStream());
		peer=incoming.getInetAddress();
		peer_port=incoming.getPort();


		if(trace) System.out.println("-- ACCEPT: incoming is " + printSocket(incoming));

		
		/** This piece of code would only accept connections from the peer address defined above. */
		if(remote.equals(incoming.getInetAddress())) {
		    if(trace)
			System.out.println("Link.run(): accepted connection from " + peer + ':' + peer_port);
		}
		else {
		    if(trace) 
			log.error("Link.run(): rejected connection request from " +
					   peer + ':' + peer_port + ". Address not specified as peer in link !");
		    closeIncomingConnection();  // only close incoming connection
		    continue;
		}
		
		// now try to create outgoing connection
		if(!established) {
		    createOutgoingConnection();
		}

		for (;;) { // GemStoneAddition remove coding anti-pattern while(!stop)
                  if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
		    try {
			num_bytes=instream.readInt();
			if(num_bytes == HB_PACKET) {
			    hb.receivedHeartbeat();
			    continue;
			}

			buf=new byte[num_bytes];
			instream.readFully(buf, 0, buf.length);
			hb.receivedMessage();  // equivalent to heartbeat response (HB_PACKET)
			if(receiver != null)
			    receiver.receive(buf);
		    }
		    catch(Exception ex) {  // IOException, EOFException, SocketException
			closeIncomingConnection(); // close incoming when read() fails
			break;
		    }
		}
	    }
	    catch(IOException io_ex) {
//		receiver_thread=null; GemStoneAddition
		break;
	    }
	    catch(Exception e) {
	    }
	}
    }



    @Override // GemStoneAddition
    public String toString() {
	StringBuffer ret=new StringBuffer();
	ret.append("Link <" + local_addr + ':' + local_port + " --> " +
		   remote_addr + ':' + remote_port + '>');
	ret.append(established? " (established)" : " (not established)");
	return ret.toString();
    }


    @Override // GemStoneAddition
    public boolean equals(Object other) {
	Link o;

	if(other == null)
	    return false;
	if(!(other instanceof Link))
	    return false;
	o=(Link)other;
	if(local_addr.equals(o.local_addr) && remote_addr.equals(o.remote_addr) &&
	   local_port == o.local_port && remote_port == o.remote_port)
	    return true;
	else
	    return false;
    }


    @Override // GemStoneAddition
    public int hashCode() {
	return local_addr.hashCode() + remote_addr.hashCode() + local_port + remote_port;
    }


    void startReceiverThread() {
        synchronized (this) { // GemStoneAddition
        stopReceiverThread();
	receiver_thread=new Thread(this, "Link.ReceiverThreadThread");
        receiver_thread.setDaemon(true);
	receiver_thread.start();
        }
    }

    synchronized /* GemStoneAddition */ void stopReceiverThread() {
        Thread t = receiver_thread; // GemStoneAddition
	if(t != null && t.isAlive()) {
//	    stop=true; GemStoneAddition
	    closeIncomingConnection();
            t.interrupt();
	    try {t.join(receiver_thread_join_timeout);} catch(InterruptedException e) {Thread.currentThread().interrupt(); } // GemStoneAddition
//	    stop=false; GemStoneAddition
	}
	receiver_thread=null;
    }




    /** Tries to create an outgoing connection. If successful, the heartbeat is started. Does <em>not</em>
	stop the connection establisher ! The reason is that this method is going to be called by the
	connection establisher as well, therefore it would kill itself ! */
    boolean createOutgoingConnection() {
	synchronized(outgoing_mutex) {  // serialize access with ConnectionEstablisher
	    if(established) {
		return true;
	    }
	    try {
		// create a socket to remote:remote_port, bind to local address (choose any local port);
		outgoing=new Socket(remote, remote_port, local, 0); // 0 means choose any local port
		outgoing.setSoLinger(true, 1);  // 1 second  // +++ ? needed ? it is off by default !
		outstream=new DataOutputStream(outgoing.getOutputStream());
		if(receiver != null) receiver.linkUp(local, local_port, remote, remote_port);
		established=true;

		if(trace) System.out.println("-- CREATE: outgoing is " + printSocket(outgoing));

		return true;
	    }
	    catch(Exception e) {
		established=false;
		return false;
	    }
	}
    }



    
    /** 
	Tries to create an outgoing connection. If successful, the heartbeat is started. Does <em>not</em>
	stop the connection establisher ! The reason is that this method is going to be called by the
	connection establisher as well, therefore it would kill itself !
    */
    boolean createOutgoingConnection(long timeout) {
	synchronized(outgoing_mutex) {  // serialize access with ConnectionEstablisher
	    if(established) {
		return true;
	    }
	    try {
		if(writer == null) writer=new TimedWriter();

		// create a socket to remote:remote_port, bind to local address (choose any local port);
		// outgoing=new Socket(remote, remote_port, local, 0); // 0 means choose any local port
		outgoing=writer.createSocket(local, remote, remote_port, timeout);
		outgoing.setSoLinger(true, 1);  // 1 second  // +++ ? needed ? it is off by default !
		outstream=new DataOutputStream(outgoing.getOutputStream());
		if(receiver != null) receiver.linkUp(local, local_port, remote, remote_port);
		established=true;
		if(trace) System.out.println("-- CREATE: outgoing is " + printSocket(outgoing));
		return true;
	    }
	    catch(Exception e) {
		established=false;
		return false;
	    }
	}
    }




    /** Closes the outgoing connection */
    void closeOutgoingConnection() {
	synchronized(outgoing_mutex) {
	    if(!established) {
		return;
	    }
	    if(outstream != null) {
		
		if(trace) System.out.println("-- CLOSE: outgoing is " + printSocket(outgoing));
		
		try {
		    outstream.close(); // flush data before socket is closed
		} 
		catch(Exception e) {}
		outstream=null;
	    }
	    if(outgoing != null) {
		try {
		    outgoing.close();
		} 
		catch(Exception e) {}
		outgoing=null;
	    }
	    established=false;
	    if(receiver != null) receiver.linkDown(local, local_port, remote, remote_port);
	}
    }

    
    /** When the heartbeat thread detects that the peer 'hangs' (not detected by incoming.read()), 
        then it closes the outgoing *and* incoming socket. The latter needs to be done,
        so that we can return to accept() and await a new client connection request. */
    synchronized void closeIncomingConnection() {
	if(instream != null) {
	    
	    if(trace) System.out.println("-- CLOSE: incoming is " + printSocket(incoming));
	    
	    try {instream.close();} catch(Exception e) {}
	    instream=null;
	}
	if(incoming != null) {
	    try {incoming.close();} catch(Exception e) {}
	    incoming=null;
	}
    }



    /** Close outgoing and incoming sockets. */
    synchronized void closeConnections() {

	// 1. Closes the outgoing connection. Then the connection establisher is started. The heartbeat
	//    thread cannot be stopped in here, because this method is called by it !	    
	closeOutgoingConnection();


	// 2. When the heartbeat thread detects that the peer 'hangs' (not detected by incoming.read()), 
	//    then it closes the outgoing *and* incoming socket. The latter needs to be done,
	//    so that we can return to accept() and await a new client connection request.
	closeIncomingConnection();
    }




    String printSocket(Socket s) {
	if(s == null) return "<null>";
	StringBuffer ret=new StringBuffer();
	ret.append(s.getLocalAddress().getHostName());
	ret.append(':');
	ret.append(s.getLocalPort());
	ret.append(" --> ");
	ret.append(s.getInetAddress().getHostName());
	ret.append(':');
	ret.append(s.getPort());
	return ret.toString();
    }
    









    /**
       Sends heartbeats across the link as long as we are connected (established=true). Uses a TimedWriter
       for both sending and responding to heartbeats. The reason is that a write() might hang if the
       peer has not closed its end, but the connection hangs (e.g. network partition, peer was stop-a'ed,
       ctrl-z of peer or peer's NIC was unplumbed) and the writer buffer is filled to capacity. This way,
       we don't hang sending timeouts.
     */
    class Heartbeat implements Runnable {
	Thread       thread=null;
	long         hb_timeout=10000;         // time to wait for heartbeats from peer, if not received -> boom !
	long         interval=3000; // {send a heartbeat | try to create connection} every 3 secs
//	volatile boolean      stop_hb=false; GemStoneAddition remove volatile anti-pattern
	long         last_hb=System.currentTimeMillis();
	boolean      missed_hb=false;
	final TimedWriter  timed_writer=new TimedWriter();



	public Heartbeat(long timeout, long hb_interval) {
	    this.hb_timeout=timeout;
	    this.interval=hb_interval;
	}


	public synchronized void start() {
	    stop();
//	    stop_hb=false; GemStoneAddition
	    missed_hb=false;
	    last_hb=System.currentTimeMillis();
	    thread=new Thread(this, "HeartbeatThread");
            thread.setDaemon(true);
	    thread.start();
	}


	public synchronized void interrupt() {
	    thread.interrupt();
	}


	public synchronized void stop() {
	    if(thread != null && thread.isAlive()) {
//		stop_hb=true; GemStoneAddition
		missed_hb=false;
		thread.interrupt();
		try {thread.join(hb_timeout+1000);} catch(InterruptedException e) {Thread.currentThread().interrupt();} // GemStoneAddition
		thread=null;
	    }
	}

	
	
	/**
	   When we receive a message from the peer, this means the peer is alive. Therefore we
	   update the time of the last heartbeat.
	 */
	public void receivedMessage() {
	    last_hb=System.currentTimeMillis();
	    if(missed_hb) {
		if(receiver != null) receiver.receivedHeartbeatAgain(local, local_port, remote, remote_port);
		missed_hb=false;
	    }
	}


	/** Callback, called by the Link whenever it encounters a heartbeat (HB_PACKET) */
	public void receivedHeartbeat() {
	    last_hb=System.currentTimeMillis();
	    if(missed_hb) {
		if(receiver != null) receiver.receivedHeartbeatAgain(local, local_port, remote, remote_port);
		missed_hb=false;
	    }
	}
	

	/**
	   Sends heartbeats when connection is established. Tries to establish connection when not established.
	   Switches between 'established' and 'not established' roles.
	 */
	public void run() {
	    long  diff=0, curr_time=0, num_missed_hbs=0;
	    
	    if(trace) System.out.println("heartbeat to " + remote + ':' + remote_port + " started");
	    for (;;) { // GemStoneAddition - remove volatile anti-pattern while(!stop_hb)

		if(established) {  // send heartbeats

		    // 1. Send heartbeat (use timed write)
		    if(outstream != null) {
			try {
			    timed_writer.write(outstream, HB_PACKET, 1500);
			    Thread.sleep(interval);
			}
                        catch (InterruptedException e) { // GemStoneAddition
                          break; // exit loop and thread. No need to set interrupt bit.
                        }
			catch(Exception io_ex) {       // IOException and TimedWriter.Timeout
			    closeOutgoingConnection(); // sets established to false
			    continue;
			}
		    }
		    else {
			established=false;
			continue;
		    }		
	     
		    // 2. If time of last HB received > timeout --> close connection
		    curr_time=System.currentTimeMillis();
		    diff=curr_time - last_hb;

		    if(curr_time - last_hb > interval) {
			num_missed_hbs=(curr_time - last_hb) / interval;
			if(receiver != null)
			    receiver.missedHeartbeat(local, local_port, remote, remote_port, (int)num_missed_hbs);
			missed_hb=true;
		    }

		    if(diff >= hb_timeout) {
			if(trace) System.out.println("###### Link.Heartbeat.run(): no heartbeat receveived for " + 
						     diff + " msecs. Closing connections. #####");
			closeConnections(); // close both incoming *and* outgoing connections
		    }
		}
		else {    // try to establish connection
		    synchronized(outgoing_mutex) { // serialize access with createOutgoingConnection()
			if(established) {
			    continue;
			}			
			try {
			    outgoing=timed_writer.createSocket(local, remote, remote_port, interval);
			    outstream=new DataOutputStream(outgoing.getOutputStream());
			    if(receiver != null) receiver.linkUp(local, local_port, remote, remote_port);
			    established=true;
			    if(trace) System.out.println("-- CREATE (CE): " + printSocket(outgoing));
			    continue;
			}
			catch(InterruptedException interrupted_ex) {
			    break; // GemStoneAddition.  Exit loop and thread
			}
			catch(Exception ex) {         // IOException, TimedWriter.Timeout
                          try { // GemStoneAddition
			    Util.sleep(interval);  // returns when done or interrupted
                          }
                          catch (InterruptedException e) {
                            break; // exit loop and thread. No need to reset interrupt.
                          }
			}
		    }
		}
	    }
	    if(trace) System.out.println("heartbeat to " + remote + ':' + remote_port + " stopped");
	    thread=null;
	}
    }
    







    protected/*GemStoneAddition*/ static class MyReceiver implements Link.Receiver {
	
	public void receive(byte[] msg) {
	    System.out.println("<-- " + new String(msg));
	}
	
	public void linkDown(InetAddress l, int lp, InetAddress r, int rp) {
	    System.out.println("** linkDown(): " + r + ':' + rp);
	}
	
	public void linkUp(InetAddress l, int lp, InetAddress r, int rp) {
	    System.out.println("** linkUp(): " + r + ':' + rp);
	}
	
	public void missedHeartbeat(InetAddress l, int lp, InetAddress r, int rp, int num) {
	    System.out.println("** missedHeartbeat(): " + r + ':' + rp);
	}
	
	public void receivedHeartbeatAgain(InetAddress l, int lp, InetAddress r, int rp) {
	    System.out.println("** receivedHeartbeatAgain(): " + r + ':' + rp);
	}
    }



//    public static void main(String[] args) {
//	String   local, remote;
//	int      local_port, remote_port;
//
//
//	if(args.length != 4) {
//	    System.err.println("\nLink <local host> <local port> <remote host> <remote port>\n");
//	    return;
//	}
//	local=args[0];
//	remote=args[2];
//	local_port=Integer.parseInt(args[1]);
//	remote_port=Integer.parseInt(args[3]);
//
//	Link l=new Link(local, local_port, remote, remote_port, new MyReceiver());
//
//	try {
//	    l.start();
//	    System.out.println(l);
//	    
//	    BufferedReader in= new BufferedReader(new InputStreamReader(System.in));	    
//	    while(true) {
//		System.out.print("> "); System.out.flush();
//		String line=in.readLine();
//		l.send(line.getBytes());
//	    }
//	}
//	catch(Exception e) {
//	    System.err.println(e);
//	}
//    }
}


