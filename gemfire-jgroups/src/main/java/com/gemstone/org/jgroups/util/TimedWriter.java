/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: TimedWriter.java,v 1.5 2005/05/30 16:14:45 belaban Exp $

package com.gemstone.org.jgroups.util;



import com.gemstone.org.jgroups.util.GemFireTracer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

/**
   Waits until the buffer has been written to the output stream, or until timeout msecs have elapsed,
   whichever comes first. 
   TODO: make it more generic, so all sorts of timed commands should be executable. Including return
   values, exceptions and Timeout exception. Also use ReusableThread instead of creating a new threa 
   each time.

   @author  Bela Ban
*/


public class TimedWriter  {
    Thread        thread=null;
    long          timeout=2000;
    boolean       completed=true;
    Exception     write_ex=null;
    Socket        sock=null;
    static GemFireTracer   log=GemFireTracer.getLog(TimedWriter.class);


    static/*GemStoneAddition*/ class Timeout extends Exception {
      private static final long serialVersionUID = -7263515891440595522L;
      @Override // GemStoneAddition
	public String toString() {
	    return "TimedWriter.Timeout";
	}
    }



    class WriterThread extends Thread  {
	DataOutputStream  out=null;
	byte[]            buf=null;
	int               i=0;

	
	public WriterThread(OutputStream out, byte[] buf) {
	    this.out=new DataOutputStream(out);
	    this.buf=buf;
	    setName("TimedWriter.WriterThread");
	}
	
	public WriterThread(OutputStream out, int i) {
	    this.out=new DataOutputStream(out);
	    this.i=i;
	    setName("TimedWriter.WriterThread");
	}

	      @Override // GemStoneAddition
	public void run() {
	    try {
		if(buf != null)
		    out.write(buf);
		else {
		    out.writeInt(i);
		}
		    
	    }
	    catch(IOException e) {
		write_ex=e;
	    }
	    completed=true;
	}
    }


    class SocketCreator extends Thread  {
	InetAddress local=null, remote=null;
	int         peer_port=0;

	
	public SocketCreator(InetAddress local, InetAddress remote, int peer_port) {
	    this.local=local;
	    this.remote=remote;
	    this.peer_port=peer_port;
	}


	    @Override // GemStoneAddition
	public void run() {
	    completed=false;
	    sock=null;

	    try {
		sock=new Socket(remote, peer_port, local, 0); // 0 means choose any port
	    }
	    catch(IOException io_ex) {
		write_ex=io_ex;
	    }
	    completed=true;
	}
    }




    void start(InetAddress local, InetAddress remote, int peer_port) {
	stop();
	thread=new SocketCreator(local, remote, peer_port);
	thread.start();
    }


    void start(OutputStream out, byte[] buf) {
	stop();
	thread=new WriterThread(out, buf);
	thread.start();
    }


    void start(OutputStream out, int i) {
	stop();
	thread=new WriterThread(out, i);
	thread.start();
    }



    void stop() {
	if(thread != null && thread.isAlive()) {
	    thread.interrupt();
	    try {thread.join(timeout);}
	    catch(InterruptedException e) {Thread.currentThread().interrupt();} // GemStoneAddition
	}
    }

    
    /**
       Writes data to an output stream. If the method does not return within timeout milliseconds,
       a Timeout exception will be thrown.
     */
    public synchronized void write(OutputStream out, byte[] buf, long timeout)
	throws Exception, Timeout, InterruptedException {
      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
	if(out == null || buf == null) {
	    log.error(ExternalStrings.TimedWriter_TIMEDWRITERWRITE_OUTPUT_STREAM_OR_BUFFER_IS_NULL_IGNORING_WRITE);
	    return;
	}

	try {
	    this.timeout=timeout;
	    completed=false;
	    start(out, buf);	    
	    if(thread == null) 
		return;
	    
	    thread.join(timeout);

	    if(completed == false) {
		throw new Timeout();
	    }
	    if(write_ex != null) {
		Exception tmp=write_ex;
		write_ex=null;
		throw tmp;
	    }
	}
	finally {   // stop the thread in any case
	    stop();
	}
    }


    public synchronized void write(OutputStream out, int i, long timeout) 
	throws Exception, Timeout, InterruptedException {
      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
	if(out == null) {
	    log.error(ExternalStrings.TimedWriter_TIMEDWRITERWRITE_OUTPUT_STREAM_IS_NULL_IGNORING_WRITE);
	    return;
	}

	try {
	    this.timeout=timeout;
	    completed=false;
	    start(out, i);
	    if(thread == null) 
		return;
	    
	    thread.join(timeout);
	    if(completed == false) {
		throw new Timeout();
	    }
	    if(write_ex != null) {
		Exception tmp=write_ex;
		write_ex=null;
		throw tmp;
	    }
	}
	finally {   // stop the thread in any case
	    stop();
	}
    }


    /** Tries to create a socket to remote_peer:remote_port. If not sucessful within timeout
	milliseconds, throws the Timeout exception. Otherwise, returns the socket or throws an
	IOException. */
    public synchronized Socket createSocket(InetAddress local, InetAddress remote, int port, long timeout) 
	throws Exception, Timeout, InterruptedException {

      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
	try {
	    this.timeout=timeout;
	    completed=false;
	    start(local, remote, port);
	    if(thread == null) 
		return null;
	    
	    thread.join(timeout);
	    if(completed == false) {
		throw new Timeout();
	    }
	    if(write_ex != null) {
		Exception tmp=write_ex;
		write_ex=null;
		throw tmp;
	    }
	    return sock;
	}
	finally {   // stop the thread in any case
	    stop();
	}
    }




//    public static void main(String[] args) {
//        TimedWriter  w=new TimedWriter();
//        InetAddress  local=null;
//        InetAddress  remote=null;
//        int          port=0;
//        Socket       sock=null ;
//
//        if(args.length != 3) {
//            log.error(JGroupsStrings.TimedWriter_TIMEDWRITER_LOCAL_HOST_REMOTE_HOST_REMOTE_PORT);
//            return;
//        }
//
//        try {
//            local=InetAddress.getByName(args[0]);
//            remote=InetAddress.getByName(args[1]);
//            port=Integer.parseInt(args[2]);
//        }
//        catch(Exception e) {
//            log.error(JGroupsStrings.TimedWriter_COULD_FIND_HOST__0, remote);
//            return;
//        }
//
//        while(true) {
//
//            try {
//                sock=w.createSocket(local, remote, port, 3000);
//                if(sock != null) {
//                    System.out.println("Connection created");
//                    return;
//                }
//            }
//            catch(TimedWriter.Timeout t) {
//                log.error(JGroupsStrings.TimedWriter_TIMED_OUT_CREATING_SOCKET);
//            }
//            catch (InterruptedException e) { // GemStoneAddition
//              return; // we're in a main; just get out
//            }
//            catch(Exception io_ex) {
//                log.error(JGroupsStrings.TimedWriter_CONNECTION_COULD_NOT_BE_CREATED_RETRYING);
//                try { // GemStoneAddition
//                Util.sleep(2000);
//                }
//                catch (InterruptedException e) {
//                  return; // we're in a main; just get out...
//                }
//            }
//        }
//
//    }
}
